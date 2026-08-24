//! What a transfer says about itself while it is still running.
//!
//! A publish and a restore are the two operations in this protocol that take
//! hours, and until this module existed both were silent for every one of them:
//! a caller learned what happened when it was over, from an [`Inscription`] or
//! a summary. This is the seam that makes the middle visible, and it is
//! deliberately the *only* one — there is no second observer on the profile
//! side and no `tracing` subscriber standing in for one.
//!
//! [`Inscription`]: crate::Inscription
//!
//! ## It carries callbacks, never counters
//!
//! Nothing here accumulates. [`Observer`] holds a handle and forwards; an
//! [`Event`] carries values the code emitting it already had in hand — the
//! layer it is on, the size the manifest states, the bytes a chunk moved. A
//! number this module would have to compute for itself is a number that does
//! not belong in it, because a transport that keeps a tally the caller does not
//! ask for is a transport whose cost nobody chose.
//!
//! ## Two emitters, because the numbers live in two places
//!
//! Neither half is honest alone:
//!
//! - the **profile driver** owns the layer loops, so it is the only code that
//!   knows *n* of *m*, a layer's kind and scope, and whether it was produced,
//!   inherited or skipped;
//! - the **transport** owns the bytes. A publish stages a layer and then
//!   uploads it in chunks, and a restore pulls a whole blob to scratch before
//!   the first record comes back out of it — so an observer wired only to the
//!   driver reports nothing for the entire duration of a download, which on the
//!   restore side is the entire operation.
//!
//! The two halves are not in step, and on the publish side they are
//! deliberately not: the transport moves a layer's blob concurrently with the
//! driver building the next one, so a blob's bytes arrive after the driver has
//! already closed the layer they belong to. Everything is reported before the
//! operation returns — that is what the seal's join buys — and nothing about
//! the order in between is promised.
//!
//! The transports answer through [`SteleWriter::observe`] and
//! [`SteleReader::observe`], which have default no-op bodies: reporting is
//! something a transport *may* do, not a tax every implementation pays.
//!
//! [`SteleWriter::observe`]: crate::SteleWriter::observe
//! [`SteleReader::observe`]: crate::SteleReader::observe
//!
//! ## Rendering is nobody's business here
//!
//! An [`Event`] is a fact, not a line of output. Which bar moves, whether
//! anything is drawn at all, and what a human reads is the binary's, which is
//! why the default is silence and why the handle is passed as an argument
//! rather than installed globally.

use std::sync::Arc;

/// Somewhere to report a transfer's progress to.
///
/// One method, taking one enum, for a reason worth stating: a trait with a
/// method per event kind would make every new fact a breaking change for every
/// implementor, and the implementors are renderers — the least interesting code
/// to have to revisit. Matching an enum, a renderer that does not care about a
/// variant writes one arm.
///
/// `Send + Sync` because a transport holds it behind an [`Arc`] and calls it
/// from wherever the bytes happen to be moving.
pub trait Progress: Send + Sync {
    fn on(&self, event: Event<'_>);
}

/// A handle on whatever is watching, and silence by default.
///
/// Cheap to clone — an [`Arc`] bump — because both a driver and a transport
/// hold one for the same run. [`Observer::silent`] is the whole of "a caller
/// that passes nothing": every emission becomes a branch on `None`, and the
/// output is byte-for-byte what it was before this seam existed.
#[derive(Clone, Default)]
pub struct Observer(Option<Arc<dyn Progress>>);

impl Observer {
    /// An observer nobody is listening to.
    pub fn silent() -> Self {
        Self(None)
    }

    /// Report to `progress`.
    pub fn new(progress: Arc<dyn Progress>) -> Self {
        Self(Some(progress))
    }

    /// Whether anything is listening.
    ///
    /// For an emitter deciding whether a *costly* event is worth assembling —
    /// never for deciding whether to do the work, which must not depend on who
    /// is watching.
    pub fn is_silent(&self) -> bool {
        self.0.is_none()
    }

    pub fn emit(&self, event: Event<'_>) {
        if let Some(progress) = &self.0 {
            progress.on(event);
        }
    }
}

/// What was resolved, and nothing about who is listening.
impl std::fmt::Debug for Observer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.is_silent() {
            true => f.write_str("Observer(silent)"),
            false => f.write_str("Observer(watching)"),
        }
    }
}

/// How a driver was done with a layer.
///
/// Three outcomes rather than a boolean because the two ways of *not* moving a
/// layer cost different things and mean different things to whoever is
/// watching: an inherited layer was never read out of a store at all, while a
/// skipped one is work an earlier attempt already paid for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The driver read the layer end to end: built out of the stores on a
    /// publish, streamed into them on a restore.
    ///
    /// Whether its blob then crossed the wire is a separate question, and the
    /// transport answers it with [`Event::Blob`] — a layer a publisher builds
    /// and a registry turns out to already hold is `Transferred` here and
    /// `Blob { moved: false, .. }` there.
    Transferred,

    /// Nothing was done for it, because it was already done: a layer an
    /// interrupted restore had committed before it died.
    Skipped,

    /// Adopted whole from the stele before it, and never read out of a store.
    Inherited,
}

/// One thing that happened, as the code that did it saw it.
///
/// Deltas rather than running totals throughout ([`Event::Records`],
/// [`Event::Bytes`]): a total is state, and state is what this seam does not
/// keep. A renderer that wants one adds them up, which is what a renderer is
/// for.
#[derive(Debug, Clone, Copy)]
pub enum Event<'a> {
    /// A layer is now in flight: the `index`-th of `total`, counting from zero.
    ///
    /// `scope` is the profile's own opaque description of what the layer covers
    /// — the epoch, the shard — carried so a watcher can name the layer the way
    /// the inscription does rather than by its position alone.
    LayerStarted {
        index: usize,
        total: usize,
        kind: &'a str,
        scope: &'a serde_json::Value,
    },

    /// The layer at `index` is done, one way or another.
    ///
    /// Carries the index rather than relying on the last
    /// [`Event::LayerStarted`] because a profile may hold several layers
    /// open at once — this one's sixteen state shards are written in a
    /// single pass over the store — so "the layer in flight" is not always
    /// a single thing.
    LayerFinished {
        index: usize,
        total: usize,
        kind: &'a str,
        outcome: Outcome,
    },

    /// Records that went past since the last one of these.
    ///
    /// Batched at the emitter's own cadence rather than one per record: the
    /// point is a bar that moves inside a layer that takes minutes, and a
    /// virtual call per record on a mainnet store would be paying for
    /// resolution nobody can see.
    Records(u64),

    /// One layer's blob, announced before the transport handles it.
    ///
    /// `bytes` is its compressed size, which the transport knows up front in
    /// both directions — from the digest pipeline on the way up, from the
    /// manifest on the way down — so a watcher can size the transfer it is
    /// about to see. `moved` is false when nothing will cross the wire because
    /// the far side already holds it, and no [`Event::Bytes`] follows for it.
    ///
    /// **Several of these can be outstanding at once, and one is not finished
    /// when the next arrives.** A publish runs its layer round trips
    /// concurrently, so what this announces is one more blob the transfer has
    /// taken on rather than the blob the transfer is now on. A renderer that
    /// reset a per-blob bar here would show eight uploads fighting over one
    /// bar; the shape that reads correctly is a running total, and the totals
    /// are exact because every announced blob is accounted for before the
    /// operation that announced it returns.
    Blob { moved: bool, bytes: u64 },

    /// Compressed bytes that crossed the wire since the last one of these.
    ///
    /// Across every blob the transport currently has in flight, and not for the
    /// blob the most recent [`Event::Blob`] announced — see there. A publish
    /// that is uploading eight layers at once reports one stream of deltas,
    /// because the thing an operator is watching is the link and not one of the
    /// eight.
    Bytes(u64),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorder(Mutex<Vec<u64>>);

    impl Progress for Recorder {
        fn on(&self, event: Event<'_>) {
            if let Event::Bytes(n) = event {
                self.0.lock().unwrap().push(n);
            }
        }
    }

    /// The property every silent call site depends on: emitting into silence
    /// does nothing and costs a branch.
    #[test]
    fn a_silent_observer_swallows_everything() {
        let observer = Observer::silent();

        assert!(observer.is_silent());
        observer.emit(Event::Bytes(1));
        observer.emit(Event::Records(1));

        assert!(Observer::default().is_silent());
    }

    /// And a clone reports to the same place, which is what lets a driver and a
    /// transport share one run's observer.
    #[test]
    fn a_clone_reports_to_the_same_place() {
        let recorder = Arc::new(Recorder::default());
        let observer = Observer::new(recorder.clone());

        assert!(!observer.is_silent());

        observer.emit(Event::Bytes(1));
        observer.clone().emit(Event::Bytes(2));

        assert_eq!(*recorder.0.lock().unwrap(), vec![1, 2]);
    }
}
