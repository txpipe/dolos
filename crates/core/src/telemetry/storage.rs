//! Storage instruments recorded by the domain: commit and housekeeping
//! durations. The instruments are created on first use, which comes after
//! the meter provider is installed.

use std::{sync::OnceLock, time::Instant};

use opentelemetry::{global, metrics::Histogram, KeyValue};

use super::{DURATION_BOUNDARIES, METER, OUTCOME, STORE};

/// The histogram of one store commit's duration.
pub const COMMIT_DURATION: &str = "dolos.storage.commit.duration";

/// The histogram of one housekeeping round's duration.
pub const HOUSEKEEPING_DURATION: &str = "dolos.storage.housekeeping.duration";

struct Instruments {
    commit: Histogram<f64>,
    housekeeping: Histogram<f64>,
}

fn instruments() -> &'static Instruments {
    static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter(METER);
        let histogram = |name: &'static str, description: &'static str| {
            meter
                .f64_histogram(name)
                .with_unit("s")
                .with_description(description)
                .with_boundaries(DURATION_BOUNDARIES.to_vec())
                .build()
        };

        Instruments {
            commit: histogram(COMMIT_DURATION, "The time one commit to a store took"),
            housekeeping: histogram(
                HOUSEKEEPING_DURATION,
                "The time one bounded housekeeping round took",
            ),
        }
    })
}

/// Runs `commit`, recording its duration as a commit to `store`, whether
/// it succeeds or not.
pub(crate) fn time_commit<T>(store: &'static str, commit: impl FnOnce() -> T) -> T {
    let started = Instant::now();
    let result = commit();

    let took = started.elapsed().as_secs_f64();
    instruments()
        .commit
        .record(took, &[KeyValue::new(STORE, store)]);

    result
}

/// Runs one housekeeping round, recording its duration with its outcome:
/// `done` when nothing is left to prune, `pending` when work is left, or
/// `error`.
pub(crate) fn time_housekeeping<E>(round: impl FnOnce() -> Result<bool, E>) -> Result<bool, E> {
    let started = Instant::now();
    let result = round();

    let outcome = match &result {
        Ok(true) => "done",
        Ok(false) => "pending",
        Err(_) => "error",
    };

    let took = started.elapsed().as_secs_f64();
    instruments()
        .housekeeping
        .record(took, &[KeyValue::new(OUTCOME, outcome)]);

    result
}
