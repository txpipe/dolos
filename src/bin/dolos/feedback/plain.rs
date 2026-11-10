//! A stele restore reported as plain lines, for a stderr that is not a
//! terminal.
//!
//! Bars draw nothing without a terminal, so a restore run from a container's
//! init step would otherwise log nothing between its source and its summary.
//! This renderer writes a timestamped line when the first layer starts, every
//! [`PERIOD`] after that whether or not anything moved, on every retry, and at
//! the end.

use std::{
    io::Write,
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use indicatif::BinaryBytes;

use dolos_snapshot::progress::{Event, Observer, Outcome, Progress};

use super::SteleProgress;

/// How often a periodic line is written once the first layer has started.
pub const PERIOD: Duration = Duration::from_secs(30);

/// How often the ticker checks whether a periodic line is due.
const TICK: Duration = Duration::from_secs(1);

/// What the lines say, as a function of the events and of the time they
/// arrived at.
///
/// Time is passed in as the elapsed time since the report began, so the
/// renderer's output is a pure function of its inputs and a test can drive it
/// with a fake clock.
pub struct Report {
    began: DateTime<Utc>,
    period: Duration,
    started: Option<Duration>,
    total: usize,
    done: usize,
    skipped: usize,
    open: Vec<(usize, String)>,
    bytes: u64,
    last: (Duration, u64),
    next: Duration,
}

impl Report {
    /// A report whose periodic lines come every `period`.
    pub fn new(began: DateTime<Utc>, period: Duration) -> Self {
        Self {
            began,
            period,
            started: None,
            total: 0,
            done: 0,
            skipped: 0,
            open: Vec::new(),
            bytes: 0,
            last: (Duration::ZERO, 0),
            next: period,
        }
    }

    /// Takes one event in; returns the line it earns, if any.
    pub fn on(&mut self, event: Event<'_>, at: Duration) -> Option<String> {
        match event {
            Event::LayerStarted {
                index,
                total,
                kind,
                scope,
            } => {
                self.total = total;
                self.open
                    .push((index, SteleProgress::describe(kind, scope)));

                if self.started.is_none() {
                    self.started = Some(at);
                    self.last = (at, self.bytes);
                    self.next = at + self.period;

                    return Some(self.line(at, format!("restoring {total} layers")));
                }
            }

            Event::LayerFinished {
                index,
                total,
                outcome,
                ..
            } => {
                self.total = total;
                self.done += 1;
                self.open.retain(|(open, _)| *open != index);

                if outcome == Outcome::Skipped {
                    self.skipped += 1;
                }
            }

            // Bytes that crossed the wire, including what a failed round trip
            // moved before it was made again. A blob the far side already held
            // moves nothing, so its announcement adds nothing here.
            Event::Bytes(moved) => self.bytes += moved,

            Event::Retry {
                attempt,
                remaining,
                reason,
            } => {
                return Some(self.line(
                    at,
                    format!("retry {attempt} ({remaining} remaining): {reason}"),
                ));
            }

            Event::Records(_) | Event::Blob { .. } => {}
        }

        None
    }

    /// The periodic line, when one is due at `at`.
    ///
    /// Due every period from the first layer's start, events or not, so
    /// that a transfer that stalled still shows. A late tick writes one line
    /// and keeps the schedule rather than catching up.
    pub fn tick(&mut self, at: Duration) -> Option<String> {
        self.started?;

        if at < self.next {
            return None;
        }

        while self.next <= at {
            self.next += self.period;
        }

        let (since, before) = self.last;
        let interval = at.saturating_sub(since);
        let recent = rate(self.bytes - before, interval);

        self.last = (at, self.bytes);

        let mut text = format!(
            "{}, {} received, {} over the last {}, {} overall",
            self.layers(),
            BinaryBytes(self.bytes),
            recent,
            human(interval),
            self.overall(at),
        );

        if let Some((_, layer)) = self.open.last() {
            text.push_str(&format!(", at {layer}"));
        }

        if self.skipped > 0 {
            text.push_str(", resuming");
        }

        Some(self.line(at, text))
    }

    /// The closing line: totals and how long the restore took.
    pub fn end(&self, at: Duration) -> String {
        let text = format!(
            "done, {}, {} received in {}, {} overall",
            self.layers(),
            BinaryBytes(self.bytes),
            human(at),
            self.overall(at),
        );

        self.line(at, text)
    }

    fn layers(&self) -> String {
        format!(
            "{}/{} layers ({} skipped)",
            self.done, self.total, self.skipped
        )
    }

    /// The rate since the first layer started.
    fn overall(&self, at: Duration) -> String {
        let since = self.started.unwrap_or(Duration::ZERO);

        rate(self.bytes, at.saturating_sub(since))
    }

    fn line(&self, at: Duration, text: String) -> String {
        let when = self.began + chrono::Duration::from_std(at).unwrap_or_default();

        format!("{} progress: {text}", when.format("%Y-%m-%dT%H:%M:%SZ"))
    }
}

fn rate(bytes: u64, over: Duration) -> String {
    let secs = over.as_secs_f64();

    let per_sec = match secs > 0.0 {
        true => (bytes as f64 / secs) as u64,
        false => 0,
    };

    format!("{}/s", BinaryBytes(per_sec))
}

/// A duration as `1h02m03s`, `4m05s` or `6s`.
fn human(duration: Duration) -> String {
    let secs = duration.as_secs();
    let (h, m, s) = (secs / 3600, secs / 60 % 60, secs % 60);

    match (h, m) {
        (0, 0) => format!("{s}s"),
        (0, _) => format!("{m}m{s:02}s"),
        _ => format!("{h}h{m:02}m{s:02}s"),
    }
}

/// Locks a mutex whether or not a panic poisoned it: the guarded state stays
/// coherent across every write here, and a progress line is not worth
/// aborting a restore over.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Where the lines go.
pub type Sink = Box<dyn Write + Send>;

struct Lines {
    report: Report,
    sink: Sink,
}

impl Lines {
    /// Writes one line, ignoring a failed write: a log reader that went away
    /// must not take the restore with it.
    fn write(&mut self, line: &str) {
        if writeln!(self.sink, "{line}").is_err() {
            // Nothing to report it on: the sink is where it would go.
        }
    }
}

struct Shared {
    lines: Mutex<Lines>,
    stopped: Mutex<bool>,
    wake: Condvar,
    began: Instant,
}

/// The plain renderer as `bootstrap stelae` runs it: [`Report`] on the
/// monotonic clock, with a ticker thread that writes the periodic lines.
pub struct PlainProgress {
    shared: Arc<Shared>,
    ticker: Mutex<Option<JoinHandle<()>>>,
}

impl PlainProgress {
    /// The renderer on stderr, a periodic line every [`PERIOD`].
    pub fn start() -> Arc<Self> {
        Self::start_with(PERIOD, TICK, Box::new(std::io::stderr()))
    }

    /// The renderer writing to `sink`, a periodic line every `period`, with a
    /// ticker that checks every `tick` whether one is due.
    pub fn start_with(period: Duration, tick: Duration, sink: Sink) -> Arc<Self> {
        let shared = Arc::new(Shared {
            lines: Mutex::new(Lines {
                report: Report::new(Utc::now(), period),
                sink,
            }),
            stopped: Mutex::new(false),
            wake: Condvar::new(),
            began: Instant::now(),
        });

        let ticking = shared.clone();

        let ticker = std::thread::spawn(move || {
            let mut stopped = lock(&ticking.stopped);

            while !*stopped {
                stopped = ticking
                    .wake
                    .wait_timeout(stopped, tick)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;

                if *stopped {
                    break;
                }

                let at = ticking.began.elapsed();
                let mut lines = lock(&ticking.lines);

                if let Some(line) = lines.report.tick(at) {
                    lines.write(&line);
                }
            }
        });

        Arc::new(Self {
            shared,
            ticker: Mutex::new(Some(ticker)),
        })
    }

    pub fn observer(self: &Arc<Self>) -> Observer {
        Observer::new(self.clone())
    }

    /// Stops the ticker and writes the end line.
    pub fn finish(&self) {
        if self.stop() {
            let at = self.shared.began.elapsed();
            let mut lines = lock(&self.shared.lines);
            let end = lines.report.end(at);

            lines.write(&end);
        }
    }

    /// Stops the ticker; true the first time only.
    fn stop(&self) -> bool {
        let Some(ticker) = lock(&self.ticker).take() else {
            return false;
        };

        *lock(&self.shared.stopped) = true;
        self.shared.wake.notify_all();
        if ticker.join().is_err() {
            tracing::warn!("the progress ticker panicked");
        }

        true
    }
}

impl Drop for PlainProgress {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Progress for PlainProgress {
    fn on(&self, event: Event<'_>) {
        let at = self.shared.began.elapsed();
        let mut lines = lock(&self.shared.lines);

        if let Some(line) = lines.report.on(event, at) {
            lines.write(&line);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn began() -> DateTime<Utc> {
        DateTime::from_timestamp(1_762_776_000, 0).unwrap()
    }

    /// A restore of four layers driven on a fake clock: the first skipped as a
    /// resumed run skips it, a retry part way, and a stretch where nothing
    /// arrives at all.
    #[test]
    fn the_plain_renderer_reports_a_resumed_run_on_a_fake_clock() {
        let mut report = Report::new(began(), PERIOD);
        let epoch = serde_json::json!({"networkMagic": 2, "epoch": 7});
        let shard = serde_json::json!({"networkMagic": 2, "epoch": 7, "shard": 3});

        assert_eq!(report.tick(secs(5)), None, "nothing before the first layer");

        let start = report.on(
            Event::LayerStarted {
                index: 0,
                total: 4,
                kind: "blocks",
                scope: &epoch,
            },
            secs(10),
        );
        assert_eq!(
            start.as_deref(),
            Some("2025-11-10T12:00:10Z progress: restoring 4 layers")
        );

        let finished = report.on(
            Event::LayerFinished {
                index: 0,
                total: 4,
                kind: "blocks",
                outcome: Outcome::Skipped,
            },
            secs(11),
        );
        assert_eq!(finished, None);

        let second = report.on(
            Event::LayerStarted {
                index: 1,
                total: 4,
                kind: "state",
                scope: &shard,
            },
            secs(12),
        );
        assert_eq!(second, None, "the start line comes once");
        report.on(
            Event::Blob {
                moved: true,
                bytes: 3 << 20,
            },
            secs(12),
        );
        report.on(Event::Bytes(3 << 20), secs(30));

        assert_eq!(report.tick(secs(39)), None, "not yet due");

        assert_eq!(
            report.tick(secs(40)).as_deref(),
            Some(
                "2025-11-10T12:00:40Z progress: 1/4 layers (1 skipped), 3.00 MiB received, \
                 102.40 KiB/s over the last 30s, 102.40 KiB/s overall, \
                 at state epoch 7 shard 3, resuming"
            )
        );

        let retry = report.on(
            Event::Retry {
                attempt: 1,
                remaining: 3,
                reason: "the registry said 500",
            },
            secs(55),
        );
        assert_eq!(
            retry.as_deref(),
            Some("2025-11-10T12:00:55Z progress: retry 1 (3 remaining): the registry said 500")
        );

        // No event between 55 s and 70 s: the line still comes, and its
        // interval rate says nothing moved.
        assert_eq!(
            report.tick(secs(70)).as_deref(),
            Some(
                "2025-11-10T12:01:10Z progress: 1/4 layers (1 skipped), 3.00 MiB received, \
                 0 B/s over the last 30s, 51.20 KiB/s overall, \
                 at state epoch 7 shard 3, resuming"
            )
        );

        report.on(
            Event::LayerFinished {
                index: 1,
                total: 4,
                kind: "state",
                outcome: Outcome::Transferred,
            },
            secs(80),
        );

        for index in 2..4 {
            report.on(
                Event::LayerStarted {
                    index,
                    total: 4,
                    kind: "logs",
                    scope: &epoch,
                },
                secs(80),
            );
            report.on(Event::Bytes(1 << 20), secs(85));
            report.on(
                Event::LayerFinished {
                    index,
                    total: 4,
                    kind: "logs",
                    outcome: Outcome::Transferred,
                },
                secs(90),
            );
        }

        assert_eq!(
            report.end(secs(130)),
            "2025-11-10T12:02:10Z progress: done, 4/4 layers (1 skipped), 5.00 MiB received in 2m10s, \
             42.67 KiB/s overall"
        );
    }

    /// A tick that comes late writes one line and keeps to the schedule rather
    /// than writing every line it missed.
    #[test]
    fn a_late_tick_writes_one_line_and_keeps_the_schedule() {
        let mut report = Report::new(began(), PERIOD);
        let scope = serde_json::json!({"epoch": 1});

        report.on(
            Event::LayerStarted {
                index: 0,
                total: 1,
                kind: "blocks",
                scope: &scope,
            },
            secs(0),
        );

        assert!(report.tick(secs(95)).is_some());
        assert_eq!(report.tick(secs(96)), None);
        assert_eq!(report.tick(secs(119)), None);
        assert!(report.tick(secs(120)).is_some());
    }

    /// With two layers open, finishing the later one puts the periodic line
    /// back at the earlier one.
    #[test]
    fn the_current_layer_is_the_latest_still_open() {
        let mut report = Report::new(began(), PERIOD);
        let first = serde_json::json!({"epoch": 1});
        let second = serde_json::json!({"epoch": 2});

        for (index, scope) in [(0, &first), (1, &second)] {
            report.on(
                Event::LayerStarted {
                    index,
                    total: 2,
                    kind: "blocks",
                    scope,
                },
                secs(0),
            );
        }

        report.on(
            Event::LayerFinished {
                index: 1,
                total: 2,
                kind: "blocks",
                outcome: Outcome::Transferred,
            },
            secs(1),
        );

        let line = report.tick(PERIOD).unwrap();
        assert!(line.contains("1/2 layers"), "{line}");
        assert!(line.ends_with(", at blocks epoch 1"), "{line}");
    }

    /// A sink the test reads back.
    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl Buffer {
        fn lines(&self) -> Vec<String> {
            String::from_utf8(self.0.lock().unwrap().clone())
                .unwrap()
                .lines()
                .map(str::to_owned)
                .collect()
        }
    }

    impl Write for Buffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// A sink whose reader went away.
    struct Closed;

    impl Write for Closed {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
    }

    fn layer_started(scope: &serde_json::Value) -> Event<'_> {
        Event::LayerStarted {
            index: 0,
            total: 1,
            kind: "blocks",
            scope,
        }
    }

    /// The ticker thread writes the periodic lines with no event arriving,
    /// and `finish` stops it and writes the end line last, once.
    #[test]
    fn the_ticker_writes_periodic_lines_and_finish_ends_them() {
        let buffer = Buffer::default();
        let period = Duration::from_millis(40);
        let progress =
            PlainProgress::start_with(period, Duration::from_millis(5), Box::new(buffer.clone()));
        let scope = serde_json::json!({"epoch": 1});

        progress.on(layer_started(&scope));

        // Waits for the ticker rather than for a fixed time, so a loaded
        // machine slows the test down instead of failing it.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !buffer
            .lines()
            .iter()
            .any(|line| line.contains(" over the last "))
        {
            assert!(Instant::now() < deadline, "no periodic line was written");
            std::thread::sleep(period);
        }

        progress.finish();
        progress.finish();
        drop(progress);

        let lines = buffer.lines();
        let count = |needle: &str| lines.iter().filter(|line| line.contains(needle)).count();

        assert_eq!(count(" progress: restoring 1 layers"), 1, "{lines:#?}");
        assert!(count(" over the last ") >= 1, "{lines:#?}");
        assert_eq!(count(" progress: done, "), 1, "{lines:#?}");
        assert!(
            lines.last().unwrap().contains(" progress: done, "),
            "{lines:#?}"
        );
    }

    /// A failed write loses the line and nothing else: the events, the ticker
    /// and the end line carry on as if it had been read.
    #[test]
    fn a_sink_that_fails_does_not_stop_the_restore() {
        let period = Duration::from_millis(10);
        let progress =
            PlainProgress::start_with(period, Duration::from_millis(2), Box::new(Closed));
        let scope = serde_json::json!({"epoch": 1});

        progress.on(layer_started(&scope));
        std::thread::sleep(period * 3);
        progress.on(Event::Retry {
            attempt: 1,
            remaining: 2,
            reason: "reset",
        });
        progress.finish();

        let lines = lock(&progress.shared.lines);
        assert_eq!(lines.report.done, 0);
        assert!(lines.report.started.is_some());
    }

    #[test]
    fn a_duration_reads_in_its_largest_units() {
        assert_eq!(human(secs(6)), "6s");
        assert_eq!(human(secs(245)), "4m05s");
        assert_eq!(human(secs(3723)), "1h02m03s");
    }
}
