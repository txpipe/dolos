//! Sync instruments recorded by the domain: work-unit durations and rollback
//! depths. The instruments are created on first use, which comes after the
//! meter provider is installed.

use std::{sync::OnceLock, time::Instant};

use opentelemetry::{global, metrics::Histogram, KeyValue};

use super::{DURATION_BOUNDARIES, METER, WORK_UNIT};

/// The histogram of one work unit's duration.
pub const WORK_UNIT_DURATION: &str = "dolos.sync.work_unit.duration";

/// The histogram of the blocks one rollback undid.
pub const ROLLBACK_DEPTH: &str = "dolos.sync.rollback.depth";

/// Bucket boundaries, in blocks, of the rollback depth: up to the security
/// parameter of the Cardano networks.
pub const ROLLBACK_DEPTH_BOUNDARIES: [f64; 10] =
    [1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0, 100.0, 500.0, 2160.0];

struct Instruments {
    work_unit: Histogram<f64>,
    rollback_depth: Histogram<u64>,
}

fn instruments() -> &'static Instruments {
    static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter(METER);

        Instruments {
            work_unit: meter
                .f64_histogram(WORK_UNIT_DURATION)
                .with_unit("s")
                .with_description("The time one sync work unit took")
                .with_boundaries(DURATION_BOUNDARIES.to_vec())
                .build(),
            rollback_depth: meter
                .u64_histogram(ROLLBACK_DEPTH)
                .with_unit("{block}")
                .with_description("The blocks one rollback undid")
                .with_boundaries(ROLLBACK_DEPTH_BOUNDARIES.to_vec())
                .build(),
        }
    })
}

/// Runs the work unit `name`, recording its duration whether it succeeds or
/// not.
pub(crate) fn time_work_unit<T>(name: &'static str, run: impl FnOnce() -> T) -> T {
    let started = Instant::now();
    let result = run();

    let took = started.elapsed().as_secs_f64();
    instruments()
        .work_unit
        .record(took, &[KeyValue::new(WORK_UNIT, name)]);

    result
}

/// Records one rollback that undid `blocks` blocks, at least one.
pub(crate) fn record_rollback(blocks: usize) {
    instruments().rollback_depth.record(blocks as u64, &[]);
}
