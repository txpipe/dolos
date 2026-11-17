//! Captures what instruments record, for tests that assert on metrics.
//!
//! A [`MetricsCapture`] owns an in-memory meter provider. Build instruments
//! on [`MetricsCapture::meter`], then read every point with
//! [`MetricsCapture::collect`]. Points are cumulative.

use std::collections::BTreeMap;

use opentelemetry::{
    metrics::{Meter, MeterProvider as _},
    KeyValue,
};
use opentelemetry_sdk::metrics::{
    data::{AggregatedMetrics, MetricData},
    InMemoryMetricExporter, SdkMeterProvider,
};

/// One histogram point.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramPoint {
    pub count: u64,
    pub sum: f64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub bounds: Vec<f64>,
}

/// One exported point: a sum's or a gauge's value, or a histogram.
#[derive(Debug, Clone, PartialEq)]
pub enum Point {
    Value(f64),
    Histogram(HistogramPoint),
}

/// Every point of one collection, by instrument name and attributes.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    points: BTreeMap<(String, String), Point>,
}

/// The attributes `k=v`, sorted and joined by commas.
fn attributes_key<'a>(attributes: impl Iterator<Item = (String, String)> + 'a) -> String {
    let mut attributes: Vec<String> = attributes.map(|(k, v)| format!("{k}={v}")).collect();
    attributes.sort();
    attributes.join(",")
}

impl Snapshot {
    fn point(&self, name: &str, attributes: &[(&str, &str)]) -> Option<&Point> {
        let key = attributes_key(
            attributes
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string())),
        );

        self.points.get(&(name.to_string(), key))
    }

    /// The value of the sum or gauge `name` with exactly `attributes`.
    pub fn value(&self, name: &str, attributes: &[(&str, &str)]) -> Option<f64> {
        match self.point(name, attributes)? {
            Point::Value(value) => Some(*value),
            Point::Histogram(_) => panic!("{name} is a histogram"),
        }
    }

    /// The histogram `name` with exactly `attributes`.
    pub fn histogram(&self, name: &str, attributes: &[(&str, &str)]) -> Option<HistogramPoint> {
        match self.point(name, attributes)? {
            Point::Histogram(point) => Some(point.clone()),
            Point::Value(_) => panic!("{name} is not a histogram"),
        }
    }

    /// Every point as `name{attributes}`, sorted.
    pub fn names(&self) -> Vec<String> {
        self.points
            .keys()
            .map(|(name, attributes)| format!("{name}{{{attributes}}}"))
            .collect()
    }
}

fn key_of<'a>(attributes: impl Iterator<Item = &'a KeyValue>) -> String {
    attributes_key(attributes.map(|kv| (kv.key.to_string(), kv.value.to_string())))
}

macro_rules! values {
    ($data:expr, $name:expr, $points:expr) => {
        match $data {
            MetricData::Sum(sum) => {
                for point in sum.data_points() {
                    $points.insert(
                        ($name.to_string(), key_of(point.attributes())),
                        Point::Value(point.value() as f64),
                    );
                }
            }
            MetricData::Gauge(gauge) => {
                for point in gauge.data_points() {
                    $points.insert(
                        ($name.to_string(), key_of(point.attributes())),
                        Point::Value(point.value() as f64),
                    );
                }
            }
            _ => panic!("unexpected aggregation of {}", $name),
        }
    };
}

/// An in-memory meter provider and its exporter.
pub struct MetricsCapture {
    provider: SdkMeterProvider,
    exporter: InMemoryMetricExporter,
}

impl Default for MetricsCapture {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsCapture {
    pub fn new() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();

        Self { provider, exporter }
    }

    /// A meter of this provider.
    pub fn meter(&self) -> Meter {
        self.provider.meter("test")
    }

    /// Every point recorded so far.
    pub fn collect(&self) -> Snapshot {
        self.exporter.reset();
        self.provider
            .force_flush()
            .expect("flush the meter provider");

        let mut points = BTreeMap::new();

        for resource in self
            .exporter
            .get_finished_metrics()
            .expect("finished metrics")
        {
            for scope in resource.scope_metrics() {
                for metric in scope.metrics() {
                    let name = metric.name();

                    match metric.data() {
                        AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                            for point in histogram.data_points() {
                                points.insert(
                                    (name.to_string(), key_of(point.attributes())),
                                    Point::Histogram(HistogramPoint {
                                        count: point.count(),
                                        sum: point.sum(),
                                        min: point.min(),
                                        max: point.max(),
                                        bounds: point.bounds().collect(),
                                    }),
                                );
                            }
                        }
                        AggregatedMetrics::F64(data) => values!(data, name, points),
                        AggregatedMetrics::I64(data) => values!(data, name, points),
                        AggregatedMetrics::U64(data) => values!(data, name, points),
                    }
                }
            }
        }

        Snapshot { points }
    }
}
