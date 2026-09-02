//! Restore planning shapes and the resume checkpoint.
//!
//! The profile owns a restore's selection — a layer's `scope` is opaque to the
//! protocol, so only the profile can read an epoch out of one — and the stores
//! it writes into. What lives here is the part that is the same whatever is
//! being restored: how much a restore holds at once ([`Budget`]), what it still
//! has to do ([`Outlook`]), and where it records what it has finished
//! ([`Checkpoint`]).
//!
//! The checkpoint's rule is [`stelae::plan::Resume`]'s — a layer is done when
//! its `diffId` is recorded, which is a fact about bytes and not about the
//! stele they were published in. *Which* layers may be skipped at all is the
//! profile's half of the split, and stays with it: this type is only ever
//! asked about the layers a profile chose to ask about.

use std::path::PathBuf;

use stelae::{
    frame::Limits,
    inscription::LayerDescriptor,
    plan::{Remaining, RestoreProgress, Resume},
    progress::Outcome,
    Digest,
};
use tracing::info;

use crate::Error;

/// What a restore holds at once.
///
/// A store writer batches until `commit` and a layer arrives as a stream, so
/// nothing bounds a restore's memory except these numbers. Both commit ceilings
/// are needed and neither subsumes the other: an index record is tens of bytes
/// and only a count bounds it, while one epoch of blocks can run to gigabytes
/// and only a byte budget bounds that.
///
/// There is deliberately no `Default`: the read limits are the publishing
/// profile's ceilings, not the protocol's defaults — a restore that read under
/// a tighter limit than the publisher wrote under would refuse that profile's
/// own steles — so the profile supplies its budget.
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    /// Per-record and window bounds on the layer read itself.
    pub limits: Limits,
    /// Records accumulated before a write batch is committed.
    pub commit_records: usize,
    /// Bytes accumulated before a write batch is committed.
    pub commit_bytes: usize,
}

/// What a restore is about to do, once the stele has been read.
///
/// Returned alongside the profile's plan so a caller can report the
/// *remaining* download rather than the original one — the whole point of the
/// accounting on a resumed run.
#[derive(Debug, Clone, Copy)]
pub struct Outlook {
    /// Layers still to fetch, and what they weigh compressed.
    pub remaining: Remaining,
    /// Layers an earlier attempt had already committed.
    pub inherited: usize,
}

/// Where a restore records what it has finished, and what it inherits.
///
/// One value rather than three arguments, because the three are one idea: the
/// file, the set of layers it says are done, and the identity of the stele
/// being restored into it.
pub struct Checkpoint {
    path: PathBuf,
    resume: Resume,
    progress: RestoreProgress,
}

impl Checkpoint {
    /// Open the checkpoint at `path` for restoring the stele `identity`.
    ///
    /// The path is the caller's: the driver never derives where a node keeps
    /// its progress file. `resume` gates whether anything on disk is
    /// *honoured* — not merely whether it is read. A restore that is not
    /// resuming is starting over: it takes an empty [`Resume`] and its first
    /// checkpoint overwrites whatever was there.
    ///
    /// That asymmetry is deliberate. A progress file that outlived the stores
    /// beside it would name layers whose data is gone, and honouring one
    /// nobody asked to honour would skip them onto empty stores — a node
    /// missing a slice of data that nothing would report. The rule here means
    /// even a file that somehow survived its stores cannot do that damage.
    pub fn open(path: PathBuf, identity: Digest, resume: bool) -> Result<Self, Error> {
        let existing = match resume {
            true => RestoreProgress::load(&path)?,
            false => None,
        };

        let resume = Resume::from_progress(existing.as_ref());

        // The new identity, the old completions. The completions are what the
        // resume rule is about — content, not the document that described it —
        // and the digest is what tells a later reader which stele a
        // half-finished restore was aimed at.
        let progress = RestoreProgress {
            inscription_digest: identity,
            completed: existing.map(|p| p.completed).unwrap_or_default(),
        };

        Ok(Self {
            path,
            resume,
            progress,
        })
    }

    /// A restore that checkpoints nowhere.
    ///
    /// For a caller driving a restore without a node behind it — the test
    /// suites, above all, which compare store sets rather than resumes.
    pub fn none() -> Self {
        Self {
            path: PathBuf::new(),
            resume: Resume::none(),
            progress: RestoreProgress::new(Digest::from_bytes([0; 32])),
        }
    }

    /// What this checkpoint inherits, for the remaining-bytes accounting.
    pub fn resume(&self) -> &Resume {
        &self.resume
    }

    /// Read `descriptor`'s layer unless an earlier attempt already committed
    /// it.
    ///
    /// The one place a layer is decided about, so that the skip and the
    /// checkpoint cannot drift apart. `fetch` runs to completion — the caller
    /// commits before it returns — and only then is the layer recorded, which
    /// is what makes the record mean "committed" rather than "attempted".
    /// Returns the outcome alongside the value, rather than leaving a caller
    /// to ask the resume the same question a second time: what an observer or
    /// a summary reports has to be this decision and not a re-derivation of
    /// it.
    pub fn fetch<T: Default, E: From<Error>>(
        &mut self,
        descriptor: &LayerDescriptor,
        fetch: impl FnOnce() -> Result<T, E>,
    ) -> Result<(T, Outcome), E> {
        if self.resume.is_done(&descriptor.diff_id) {
            info!(
                kind = descriptor.kind,
                scope = %descriptor.scope,
                "skipping a layer an earlier attempt completed"
            );

            return Ok((T::default(), Outcome::Skipped));
        }

        let out = fetch()?;

        self.record(descriptor.diff_id)?;

        Ok((out, Outcome::Transferred))
    }

    fn record(&mut self, diff_id: Digest) -> Result<(), Error> {
        if self.path.as_os_str().is_empty() {
            return Ok(());
        }

        self.progress.record(diff_id);
        self.progress.save(&self.path)?;

        Ok(())
    }

    /// Delete the progress file.
    ///
    /// For the moment the restore is *finished* — which is the caller's call,
    /// not the last `fetch`'s: whatever work follows the final layer is
    /// exactly the window a kept progress file lets an operator repair by
    /// resuming.
    pub fn clear(&self) -> Result<(), Error> {
        if self.path.as_os_str().is_empty() {
            return Ok(());
        }

        RestoreProgress::remove(&self.path)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(byte: u8) -> LayerDescriptor {
        LayerDescriptor {
            kind: "blocks".into(),
            media_type: "application/cbor-seq".into(),
            diff_id: Digest::from_bytes([byte; 32]),
            records: 1,
            uncompressed_size: 1,
            scope: serde_json::json!({"epoch": 7}),
        }
    }

    #[test]
    fn a_resumed_checkpoint_skips_what_it_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("progress.json");
        let identity = Digest::from_bytes([1; 32]);

        let mut first = Checkpoint::open(path.clone(), identity, false).unwrap();
        let (_, outcome) = first.fetch::<(), Error>(&descriptor(2), || Ok(())).unwrap();
        assert!(matches!(outcome, Outcome::Transferred));

        let mut second = Checkpoint::open(path, identity, true).unwrap();
        let (_, outcome) = second
            .fetch::<(), Error>(&descriptor(2), || panic!("must not refetch"))
            .unwrap();
        assert!(matches!(outcome, Outcome::Skipped));
    }

    #[test]
    fn a_fresh_checkpoint_ignores_what_is_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("progress.json");
        let identity = Digest::from_bytes([1; 32]);

        let mut first = Checkpoint::open(path.clone(), identity, false).unwrap();
        first.fetch::<(), Error>(&descriptor(2), || Ok(())).unwrap();

        let mut fresh = Checkpoint::open(path, identity, false).unwrap();
        let mut fetched = false;
        fresh
            .fetch::<(), Error>(&descriptor(2), || {
                fetched = true;
                Ok(())
            })
            .unwrap();
        assert!(fetched);
    }

    #[test]
    fn a_checkpoint_that_goes_nowhere_records_nothing() {
        let mut none = Checkpoint::none();
        none.fetch::<(), Error>(&descriptor(2), || Ok(())).unwrap();
        assert!(none.resume().is_empty());

        none.clear().unwrap();
    }
}
