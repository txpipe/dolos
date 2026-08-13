//! A recording observer, and the invariants every event stream owes.
//!
//! What makes this worth having rather than a `Vec` per test: the assertions
//! below are about the stream's *shape* — every layer opens once and closes
//! once, the positions are exactly `0..total`, nothing is reported after the
//! last layer closes — and those hold for a publish and a restore alike. A
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

use std::sync::Mutex;

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
    /// Anything reported after the last layer closed — which is nothing, and
    /// checking it is how a byte delta escaping into the gap between layers
    /// gets caught.
    trailing: usize,
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
            } => stream.opened.push(layer(index, total, kind, scope)),

            Event::LayerFinished {
                index,
                total,
                kind,
                outcome,
            } => stream.closed.push(Closed {
                index,
                total,
                kind: kind.to_owned(),
                outcome,
            }),

            Event::Records(moved) => {
                if stream.settled() {
                    stream.trailing += 1;
                }

                stream.records += moved;
            }

            Event::Blob { moved, bytes } => {
                if stream.settled() {
                    stream.trailing += 1;
                }

                stream.blobs.push(Blob { moved, bytes });
            }

            Event::Bytes(moved) => {
                if stream.settled() {
                    stream.trailing += 1;
                }

                stream.bytes += moved;
            }
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

    pub fn records(&self) -> u64 {
        self.0.lock().unwrap().records
    }

    /// Compressed bytes the transport said crossed the wire.
    pub fn bytes(&self) -> u64 {
        self.0.lock().unwrap().bytes
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
            "records or bytes were reported after the last layer closed"
        );
    }
}
