//! Counting layers and records for the two drivers to report.
//!
//! Nothing here is protocol: [`stelae::progress`] defines the seam and the
//! events, and this is the small amount of arithmetic a *driver* has to do to
//! fill one in — which layer of how many is in flight, and how often a long
//! scan is worth mentioning.
//!
//! Shared by [`crate::export`] and [`crate::restore`] because the two count the
//! same thing and a second copy would be a second answer to "how far along is
//! this". Both types are call-scoped: they live on a stack frame for the length
//! of one publish or one restore, and neither outlives it.

use stelae::progress::{Event, Observer, Outcome};

/// Records reported in one go.
///
/// Not one event per record. A mainnet state shard is tens of millions of them,
/// and a bar redrawn per record is resolution nobody can see bought at a
/// virtual call per record; this is fine enough that a bar still moves several
/// times a second on the slowest layer in the profile.
const RECORD_CADENCE: u64 = 4096;

/// Where a driver is in its run of layers.
///
/// Positions are handed out by [`Cursor::open`] and quoted back to
/// [`Cursor::close`] rather than tracked as "the current layer", because a
/// driver may hold several open at once — the export's state pass keeps all
/// sixteen shard sinks open across one walk of the store — and a single cursor
/// would report the last one opened as the one that finished.
///
/// The position counter is atomic because the export drives its layer
/// producers from a pool of threads, and every producer announces through the
/// one cursor. Positions are display order, nothing else: the inscription
/// lists layers by its own rule, so two runs that announce in different
/// interleavings still publish the same document.
pub(crate) struct Cursor<'a> {
    observer: &'a Observer,
    next: std::sync::atomic::AtomicUsize,
    total: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(observer: &'a Observer, total: usize) -> Self {
        Self {
            observer,
            next: std::sync::atomic::AtomicUsize::new(0),
            total,
        }
    }

    /// Announce a layer and take its position in the run.
    pub(crate) fn open(&self, kind: &str, scope: &serde_json::Value) -> usize {
        let index = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.observer.emit(Event::LayerStarted {
            index,
            total: self.total,
            kind,
            scope,
        });

        index
    }

    /// Close the layer `index` was handed out for.
    pub(crate) fn close(&self, index: usize, kind: &str, outcome: Outcome) {
        self.observer.emit(Event::LayerFinished {
            index,
            total: self.total,
            kind,
            outcome,
        });
    }

    /// A record counter reporting to the same place.
    pub(crate) fn records(&self) -> Records<'a> {
        Records {
            observer: self.observer,
            pending: 0,
        }
    }

    /// Layers announced so far — what a caller cross-checks its own total
    /// against once the run is over.
    pub(crate) fn opened(&self) -> usize {
        self.next.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Records counted since the last time anyone was told about them.
///
/// [`Records::flush`] is explicit rather than a `Drop`, because the moment that
/// matters is *before* the layer closes: an observer that saw a layer finish
/// and then received records for it would have to know which layer they
/// belonged to, and the seam deliberately does not carry that.
pub(crate) struct Records<'a> {
    observer: &'a Observer,
    pending: u64,
}

impl Records<'_> {
    pub(crate) fn tick(&mut self) {
        self.pending += 1;

        if self.pending >= RECORD_CADENCE {
            self.flush();
        }
    }

    pub(crate) fn flush(&mut self) {
        if self.pending > 0 {
            self.observer.emit(Event::Records(self.pending));
            self.pending = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use stelae::progress::Progress;

    #[derive(Default)]
    struct Recorder(Mutex<Vec<String>>);

    impl Progress for Recorder {
        fn on(&self, event: Event<'_>) {
            let line = match event {
                Event::LayerStarted { index, kind, .. } => format!("open {index} {kind}"),
                Event::LayerFinished {
                    index,
                    kind,
                    outcome,
                    ..
                } => format!("close {index} {kind} {outcome:?}"),
                Event::Records(n) => format!("records {n}"),
                other => format!("{other:?}"),
            };

            self.0.lock().unwrap().push(line);
        }
    }

    /// The shape the state pass needs: positions handed out up front, closed in
    /// whatever order the sinks finish, and never confused with each other.
    #[test]
    fn positions_survive_layers_held_open_together() {
        let recorder = Arc::new(Recorder::default());
        let observer = Observer::new(recorder.clone());
        let cursor = Cursor::new(&observer, 2);

        let scope = serde_json::json!({});
        let first = cursor.open("state", &scope);
        let second = cursor.open("state", &scope);

        cursor.close(second, "state", Outcome::Transferred);
        cursor.close(first, "state", Outcome::Transferred);

        assert_eq!(cursor.opened(), 2);
        assert_eq!(
            *recorder.0.lock().unwrap(),
            vec![
                "open 0 state",
                "open 1 state",
                "close 1 state Transferred",
                "close 0 state Transferred",
            ]
        );
    }

    /// A cadence that reported nothing until a layer ended would leave the bar
    /// still for exactly the layers it exists for, and one that reported a
    /// trailing zero would tell a renderer records moved when none did.
    #[test]
    fn records_report_on_the_cadence_and_once_at_the_end() {
        let recorder = Arc::new(Recorder::default());
        let observer = Observer::new(recorder.clone());
        let cursor = Cursor::new(&observer, 1);

        let mut records = cursor.records();

        for _ in 0..RECORD_CADENCE + 3 {
            records.tick();
        }

        records.flush();
        records.flush();

        assert_eq!(
            *recorder.0.lock().unwrap(),
            vec![format!("records {RECORD_CADENCE}"), "records 3".to_owned()]
        );
    }
}
