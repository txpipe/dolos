//! An in-memory meter provider for tests that assert on exported metrics.
//!
//! Each [`MetricsRecorder`] owns its provider, so tests running in parallel
//! never see one another's samples, and nothing touches the global provider.

use opentelemetry::{
    metrics::{Meter, MeterProvider as _},
    KeyValue,
};
use opentelemetry_sdk::metrics::{
    data::{AggregatedMetrics, MetricData},
    InMemoryMetricExporter, SdkMeterProvider,
};

/// A meter provider whose samples a test reads back.
pub struct MetricsRecorder {
    provider: SdkMeterProvider,
    exporter: InMemoryMetricExporter,
}

impl Default for MetricsRecorder {
    fn default() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();

        Self { provider, exporter }
    }
}

impl MetricsRecorder {
    /// A meter of this recorder's provider.
    pub fn meter(&self, name: &'static str) -> Meter {
        self.provider.meter(name)
    }

    /// Every exported point as `name{key=value,...} value`, attributes and
    /// points sorted. Histograms show `count=<n>`; sums show their value.
    pub fn points(&self) -> Vec<String> {
        self.exporter.reset();
        self.provider.force_flush().expect("metrics flush");

        let mut points = Vec::new();

        for resource in self.exporter.get_finished_metrics().expect("metrics") {
            for scope in resource.scope_metrics() {
                for metric in scope.metrics() {
                    let mut push = |attributes: &mut dyn Iterator<Item = &KeyValue>, value| {
                        let mut attributes: Vec<String> = attributes
                            .map(|kv| format!("{}={}", kv.key, kv.value))
                            .collect();
                        attributes.sort();

                        points.push(format!(
                            "{}{{{}}} {value}",
                            metric.name(),
                            attributes.join(",")
                        ));
                    };

                    match metric.data() {
                        AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                            for point in histogram.data_points() {
                                push(&mut point.attributes(), format!("count={}", point.count()));
                            }
                        }
                        AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                            for point in sum.data_points() {
                                push(&mut point.attributes(), point.value().to_string());
                            }
                        }
                        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                            for point in sum.data_points() {
                                push(&mut point.attributes(), point.value().to_string());
                            }
                        }
                        _ => panic!("no rendering for instrument {}", metric.name()),
                    }
                }
            }
        }

        points.sort();
        points
    }

    /// The points of the instrument `name`.
    pub fn points_of(&self, name: &str) -> Vec<String> {
        let prefix = format!("{name}{{");

        self.points()
            .into_iter()
            .filter(|point| point.starts_with(&prefix))
            .collect()
    }
}
