//! Request instruments shared by every serving surface.
//!
//! A surface builds [`RequestMetrics`] once for its family, then starts a
//! [`RequestGuard`] per request with the request's bounded labels. Finishing
//! the guard records the duration; dropping it unfinished counts a dropped
//! request. Either way active requests go down exactly once. Callers pass
//! attributes: this module knows nothing of routes or transports.

use std::{sync::Arc, time::Instant};

use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter, UpDownCounter},
    KeyValue,
};

use super::{DURATION_BOUNDARIES, SURFACE};

/// The semantic-convention family a surface's requests belong to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    /// `http.server.request.duration`, `http.server.active_requests` and
    /// `dolos.http.server.request.dropped`.
    Http,
    /// `rpc.server.call.duration`, `dolos.rpc.server.active_requests` and
    /// `dolos.rpc.server.request.dropped`.
    Rpc,
}

impl Family {
    /// The duration, active and dropped instrument names.
    pub const fn names(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::Http => (
                "http.server.request.duration",
                "http.server.active_requests",
                "dolos.http.server.request.dropped",
            ),
            Self::Rpc => (
                "rpc.server.call.duration",
                "dolos.rpc.server.active_requests",
                "dolos.rpc.server.request.dropped",
            ),
        }
    }
}

struct Instruments {
    surface: KeyValue,
    duration: Histogram<f64>,
    active: UpDownCounter<i64>,
    dropped: Counter<u64>,
}

/// One surface's request instruments. Cheap to clone.
#[derive(Clone)]
pub struct RequestMetrics(Arc<Instruments>);

impl RequestMetrics {
    /// The instruments of `family` on the global meter `meter`, every sample
    /// labelled with `surface`.
    pub fn new(family: Family, meter: &'static str, surface: &'static str) -> Self {
        Self::with_meter(family, &global::meter(meter), surface)
    }

    /// The same instruments on `meter`.
    pub fn with_meter(family: Family, meter: &Meter, surface: &'static str) -> Self {
        let (duration, active, dropped) = family.names();

        Self(Arc::new(Instruments {
            surface: KeyValue::new(SURFACE, surface),
            duration: meter
                .f64_histogram(duration)
                .with_unit("s")
                .with_boundaries(DURATION_BOUNDARIES.to_vec())
                .build(),
            active: meter
                .i64_up_down_counter(active)
                .with_unit("{request}")
                .build(),
            dropped: meter.u64_counter(dropped).with_unit("{request}").build(),
        }))
    }

    /// Starts a request with `labels`, raising active requests.
    pub fn start(&self, labels: impl IntoIterator<Item = KeyValue>) -> RequestGuard {
        let mut labels: Vec<KeyValue> = labels.into_iter().collect();
        labels.push(self.0.surface.clone());

        self.0.active.add(1, &labels);

        RequestGuard {
            metrics: self.clone(),
            labels,
            started: Instant::now(),
            finished: false,
        }
    }
}

/// A request in flight; see [`RequestMetrics::start`].
#[must_use = "dropping the guard counts the request as dropped"]
pub struct RequestGuard {
    metrics: RequestMetrics,
    labels: Vec<KeyValue>,
    started: Instant,
    finished: bool,
}

impl RequestGuard {
    /// Records the request's duration with its start labels and `status`.
    pub fn finish(mut self, status: impl IntoIterator<Item = KeyValue>) {
        self.finished = true;

        let mut labels = self.labels.clone();
        labels.extend(status);

        let took = self.started.elapsed().as_secs_f64();
        self.metrics.0.duration.record(took, &labels);
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.metrics.0.dropped.add(1, &self.labels);
        }

        self.metrics.0.active.add(-1, &self.labels);
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{
        data::{AggregatedMetrics, MetricData},
        InMemoryMetricExporter, SdkMeterProvider,
    };

    use super::*;

    /// Every exported point as `name{attributes} value`, sorted.
    fn collect(provider: &SdkMeterProvider, exporter: &InMemoryMetricExporter) -> Vec<String> {
        exporter.reset();
        provider.force_flush().unwrap();

        let mut points = Vec::new();

        for resource in exporter.get_finished_metrics().unwrap() {
            for scope in resource.scope_metrics() {
                for metric in scope.metrics() {
                    let mut push = |attributes: Vec<String>, value: String| {
                        let mut attributes = attributes;
                        attributes.sort();
                        points.push(format!(
                            "{}{{{}}} {value}",
                            metric.name(),
                            attributes.join(",")
                        ));
                    };
                    let labels = |kvs: &mut dyn Iterator<Item = &KeyValue>| {
                        kvs.map(|kv| format!("{}={}", kv.key, kv.value)).collect()
                    };

                    match metric.data() {
                        AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                            for point in histogram.data_points() {
                                push(
                                    labels(&mut point.attributes()),
                                    format!("count={}", point.count()),
                                );
                            }
                        }
                        AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                            for point in sum.data_points() {
                                push(labels(&mut point.attributes()), point.value().to_string());
                            }
                        }
                        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                            for point in sum.data_points() {
                                push(labels(&mut point.attributes()), point.value().to_string());
                            }
                        }
                        _ => panic!("unexpected instrument {}", metric.name()),
                    }
                }
            }
        }

        points.sort();
        points
    }

    fn setup(family: Family) -> (SdkMeterProvider, InMemoryMetricExporter, RequestMetrics) {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let metrics = RequestMetrics::with_meter(family, &provider.meter("test"), "minibf");

        (provider, exporter, metrics)
    }

    #[test]
    fn a_finished_request_records_one_duration_and_leaves_nothing_active() {
        let (provider, exporter, metrics) = setup(Family::Http);

        let guard = metrics.start([KeyValue::new("http.request.method", "GET")]);

        assert_eq!(
            collect(&provider, &exporter),
            ["http.server.active_requests{http.request.method=GET,surface=minibf} 1"]
        );

        guard.finish([KeyValue::new("http.response.status_code", 200)]);

        assert_eq!(
            collect(&provider, &exporter),
            [
                "http.server.active_requests{http.request.method=GET,surface=minibf} 0",
                "http.server.request.duration{http.request.method=GET,http.response.status_code=200,surface=minibf} count=1",
            ]
        );
    }

    #[test]
    fn a_dropped_request_counts_as_dropped_and_records_no_duration() {
        let (provider, exporter, metrics) = setup(Family::Rpc);

        let guard = metrics.start([KeyValue::new("rpc.method", "ReadTip")]);
        drop(guard);

        assert_eq!(
            collect(&provider, &exporter),
            [
                "dolos.rpc.server.active_requests{rpc.method=ReadTip,surface=minibf} 0",
                "dolos.rpc.server.request.dropped{rpc.method=ReadTip,surface=minibf} 1",
            ]
        );
    }

    #[test]
    fn the_duration_is_recorded_in_seconds_over_the_shared_boundaries() {
        let (provider, exporter, metrics) = setup(Family::Http);

        let guard = metrics.start([]);
        std::thread::sleep(std::time::Duration::from_millis(15));
        guard.finish([]);

        exporter.reset();
        provider.force_flush().unwrap();

        let metrics = exporter.get_finished_metrics().unwrap();
        let metric = metrics[0]
            .scope_metrics()
            .next()
            .unwrap()
            .metrics()
            .next()
            .unwrap();
        let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() else {
            panic!("{} is not a histogram", metric.name());
        };
        let point = histogram.data_points().next().unwrap();

        assert_eq!(point.bounds().collect::<Vec<_>>(), DURATION_BOUNDARIES);
        assert_eq!(metric.unit(), "s");
        // 15 ms recorded in seconds, not in milliseconds.
        assert!((0.015..1.0).contains(&point.sum()), "{}", point.sum());
    }
}
