//! A recording observer, and the invariants every event stream owes.
//!
//! What makes this worth having rather than a `Vec` per test: the assertions
//! below are about the stream's *shape* — every layer opens once and closes
//! once, the positions are exactly `0..total`, the driver reports nothing after
//! the last layer closes — and those hold for a publish and a restore alike. A
//! suite that only checked the numbers it happened to care about would let a
//! second emitter added later report a layer twice without anything noticing.
//!
//! The numbers themselves are cross-checked against counters that already
//! exist — `Transfer` on the publish side, `Summary` on the restore side — and
//! never against the stream itself. An event stream asserted only against its
//! own recording is a test of the recorder.

// Each integration test binary compiles this module in full, so the parts one
// binary does not reach look dead to it. They are not.
#![allow(dead_code)]

use std::{collections::BTreeMap, sync::Mutex};

use stelae::progress::{Event, Observer, Outcome, Progress};

/// One layer, as the stream described it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layer {
    pub index: usize,
    pub total: usize,
    pub kind: String,
    pub scope: String,
}

/// One layer's end, as the stream reported it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Closed {
    pub index: usize,
    pub total: usize,
    pub kind: String,
    pub outcome: Outcome,
}

/// One blob, as the transport announced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Blob {
    pub moved: bool,
    pub bytes: u64,
}

#[derive(Debug, Default)]
struct Stream {
    opened: Vec<Layer>,
    closed: Vec<Closed>,
    records: u64,
    blobs: Vec<Blob>,
    bytes: u64,
    /// Round trips the transport made again after the registry failed one.
    ///
    /// Recorded because it is the one thing that unpicks the byte cross-check
    /// below: a re-sent blob crosses the wire twice and is counted once, so a
    /// run that retried moved more bytes than it uploaded. Against these
    /// fixtures it is always zero, and a suite that asserts that is a suite
    /// that would notice a retry loop firing where nothing failed.
    retries: usize,
    /// Anything the *driver* reported after the last layer closed — which is
    /// nothing, and checking it is how a record delta escaping into the gap
    /// between layers gets caught.
    ///
    /// The driver's events only, because the transport's are no longer in step
    /// with them and are not meant to be: a publish uploads a layer's blob
    /// concurrently with the driver building the next one, so a blob announced
    /// or a byte moved after the last layer closed is the publish still
    /// finishing rather than an emitter that has lost its place. What holds for
    /// those is that they have all arrived by the time the operation returns,
    /// and the totals below — cross-checked against `Transfer` — are what says
    /// so.
    trailing: usize,
    /// Layers of each kind open right now, and the most that were ever open at
    /// once. A driver that holds several open across one walk of a store — the
    /// state pass over its shards, the index pass over a band — is reporting
    /// exactly that through this seam, and the peak is the only thing in the
    /// stream that says so.
    live: BTreeMap<String, usize>,
    peak: BTreeMap<String, usize>,
}

/// Everything a run said about itself.
#[derive(Debug, Default)]
pub struct Watcher(Mutex<Stream>);

impl Progress for Watcher {
    fn on(&self, event: Event<'_>) {
        let mut stream = self.0.lock().unwrap();

        let layer = |index, total, kind: &str, scope: &serde_json::Value| Layer {
            index,
            total,
            kind: kind.to_owned(),
            scope: scope.to_string(),
        };

        match event {
            Event::LayerStarted {
                index,
                total,
                kind,
                scope,
            } => {
                stream.opened.push(layer(index, total, kind, scope));

                let live = stream.live.entry(kind.to_owned()).or_default();
                *live += 1;

                let live = *live;
                let peak = stream.peak.entry(kind.to_owned()).or_default();
                *peak = (*peak).max(live);
            }

            Event::LayerFinished {
                index,
                total,
                kind,
                outcome,
            } => {
                stream.closed.push(Closed {
                    index,
                    total,
                    kind: kind.to_owned(),
                    outcome,
                });

                // A finish with no open layer to match is exactly the
                // malformation this harness exists to catch, so it fails here
                // rather than wrapping a `usize` and leaving `peak` unreadable
                // in a release build.
                let Some(live) = stream.live.get_mut(kind) else {
                    panic!("a `{kind}` layer finished without one having started");
                };

                let Some(left) = live.checked_sub(1) else {
                    panic!("more `{kind}` layers finished than were started");
                };

                *live = left;
            }

            Event::Records(moved) => {
                if stream.settled() {
                    stream.trailing += 1;
                }

                stream.records += moved;
            }

            Event::Blob { moved, bytes } => stream.blobs.push(Blob { moved, bytes }),

            Event::Bytes(moved) => stream.bytes += moved,

            Event::Retry { .. } => stream.retries += 1,
        }
    }
}

impl Stream {
    /// Whether every layer announced has already been closed, which for a
    /// well-formed run means the transfer is over.
    fn settled(&self) -> bool {
        !self.opened.is_empty()
            && self.opened.len() == self.closed.len()
            && self.opened.len() == self.opened[0].total
    }
}

impl Watcher {
    pub fn observer(self: &std::sync::Arc<Self>) -> Observer {
        Observer::new(self.clone())
    }

    /// Layers the run announced.
    pub fn layers(&self) -> usize {
        self.0.lock().unwrap().opened.len()
    }

    /// How many layers ended each way.
    pub fn ended(&self, outcome: Outcome) -> usize {
        self.0
            .lock()
            .unwrap()
            .closed
            .iter()
            .filter(|closed| closed.outcome == outcome)
            .count()
    }

    /// The most layers of `kind` the run held open at the same time.
    pub fn peak_open(&self, kind: &str) -> usize {
        self.0.lock().unwrap().peak.get(kind).copied().unwrap_or(0)
    }

    pub fn records(&self) -> u64 {
        self.0.lock().unwrap().records
    }

    /// Compressed bytes the transport said crossed the wire.
    pub fn bytes(&self) -> u64 {
        self.0.lock().unwrap().bytes
    }

    /// Round trips the registry failed and the transport made again.
    pub fn retries(&self) -> usize {
        self.0.lock().unwrap().retries
    }

    /// Blobs the transport handled, by whether anything moved for them.
    pub fn blobs(&self, moved: bool) -> Vec<Blob> {
        self.0
            .lock()
            .unwrap()
            .blobs
            .iter()
            .copied()
            .filter(|blob| blob.moved == moved)
            .collect()
    }

    /// What the blobs of one kind weigh, as the transport stated their sizes.
    pub fn blob_bytes(&self, moved: bool) -> u64 {
        self.blobs(moved).iter().map(|blob| blob.bytes).sum()
    }

    /// The invariants a well-formed stream owes, whichever direction produced
    /// it.
    ///
    /// `expected` is the layer count from somewhere other than the stream — the
    /// inscription on a publish, the plan on a restore — because "the stream
    /// counted what the stream counted" proves nothing.
    pub fn assert_well_formed(&self, expected: usize) {
        let stream = self.0.lock().unwrap();

        assert_eq!(
            stream.opened.len(),
            expected,
            "layers announced, against the count the document carries"
        );

        assert_eq!(
            stream.closed.len(),
            expected,
            "layers closed, against the count the document carries"
        );

        let positions: Vec<usize> = {
            let mut positions: Vec<usize> = stream.opened.iter().map(|l| l.index).collect();
            positions.sort_unstable();
            positions
        };

        assert_eq!(
            positions,
            (0..expected).collect::<Vec<_>>(),
            "every position from 0 announced exactly once"
        );

        let closed: Vec<usize> = {
            let mut closed: Vec<usize> = stream.closed.iter().map(|c| c.index).collect();
            closed.sort_unstable();
            closed
        };

        assert_eq!(
            closed,
            (0..expected).collect::<Vec<_>>(),
            "every position closed exactly once"
        );

        for layer in &stream.opened {
            assert_eq!(layer.total, expected, "a layer reported a different total");
            assert!(!layer.kind.is_empty(), "a layer reported no kind");
            assert_ne!(layer.scope, "null", "a layer reported no scope");
        }

        for closed in &stream.closed {
            assert_eq!(closed.total, expected, "a layer reported a different total");
        }

        assert_eq!(
            stream.trailing, 0,
            "records were reported after the last layer closed"
        );

        let open: Vec<(&String, &usize)> =
            stream.live.iter().filter(|(_, live)| **live > 0).collect();

        assert!(
            open.is_empty(),
            "the run ended with layers still open: {open:?}"
        );
    }
}
