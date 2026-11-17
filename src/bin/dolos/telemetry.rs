//! The OTLP exporters of `[telemetry]`, the resource that names this process
//! and the meter provider.

use dolos_core::config::{OtlpProtocol, TelemetryConfig};
use miette::{Context as _, IntoDiagnostic as _};
use opentelemetry::KeyValue;
use opentelemetry_otlp::{Protocol, WithExportConfig as _, WithTonicConfig as _};
use opentelemetry_sdk::{
    metrics::{exporter::PushMetricExporter, PeriodicReader, SdkMeterProvider},
    resource::{EnvResourceDetector, TelemetryResourceDetector},
    Resource,
};
use std::time::Duration;

/// The resource attribute naming the network: its name when it is known,
/// else its magic.
const NETWORK: &str = "dolos.network";

/// Where a signal is sent: the endpoint itself over gRPC, the signal's path
/// under it over HTTP, which the exporter does not append to an endpoint
/// set in code.
fn signal_endpoint(config: &TelemetryConfig, path: &str) -> String {
    match config.otlp_protocol {
        OtlpProtocol::Grpc => config.otlp_endpoint.clone(),
        OtlpProtocol::HttpProtobuf => {
            format!("{}/{path}", config.otlp_endpoint.trim_end_matches('/'))
        }
    }
}

/// Tonic adds no trust roots on its own, so an `https` endpoint gets the
/// bundled ones.
fn tls(endpoint: &str) -> Option<opentelemetry_otlp::tonic_types::transport::ClientTlsConfig> {
    endpoint.starts_with("https://").then(|| {
        opentelemetry_otlp::tonic_types::transport::ClientTlsConfig::new().with_enabled_roots()
    })
}

/// The exporters a configuration starts.
pub struct Exporters {
    pub spans: opentelemetry_otlp::SpanExporter,
    /// None with `metrics = false`.
    pub metrics: Option<opentelemetry_otlp::MetricExporter>,
}

/// The exporters of `config`: none without `[telemetry]`. Headers come from
/// `OTEL_EXPORTER_OTLP_HEADERS`, which the exporters read when built.
pub fn exporters(config: Option<&TelemetryConfig>) -> miette::Result<Option<Exporters>> {
    let Some(config) = config else {
        return Ok(None);
    };

    Ok(Some(Exporters {
        spans: span_exporter(config)?,
        metrics: metric_exporter(config)?,
    }))
}

fn span_exporter(config: &TelemetryConfig) -> miette::Result<opentelemetry_otlp::SpanExporter> {
    let builder = opentelemetry_otlp::SpanExporter::builder();
    let endpoint = signal_endpoint(config, "v1/traces");

    let exporter = match config.otlp_protocol {
        OtlpProtocol::Grpc => {
            let builder = builder.with_tonic().with_endpoint(&endpoint);

            match tls(&endpoint) {
                Some(tls) => builder.with_tls_config(tls).build(),
                None => builder.build(),
            }
        }
        OtlpProtocol::HttpProtobuf => builder
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(endpoint)
            .build(),
    };

    exporter
        .into_diagnostic()
        .context("building OTLP span exporter")
}

fn metric_exporter(
    config: &TelemetryConfig,
) -> miette::Result<Option<opentelemetry_otlp::MetricExporter>> {
    if !config.metrics {
        return Ok(None);
    }

    let builder = opentelemetry_otlp::MetricExporter::builder();
    let endpoint = signal_endpoint(config, "v1/metrics");

    let exporter = match config.otlp_protocol {
        OtlpProtocol::Grpc => {
            let builder = builder.with_tonic().with_endpoint(&endpoint);

            match tls(&endpoint) {
                Some(tls) => builder.with_tls_config(tls).build(),
                None => builder.build(),
            }
        }
        OtlpProtocol::HttpProtobuf => builder
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(endpoint)
            .build(),
    };

    exporter
        .into_diagnostic()
        .context("building OTLP metric exporter")
        .map(Some)
}

/// The resource both providers report: `service.name` from `config`,
/// `service.instance.id` from the hostname, `service.version` from the build
/// and `dolos.network`. `OTEL_RESOURCE_ATTRIBUTES` overrides each of them.
pub fn resource(config: &TelemetryConfig, network: String) -> Resource {
    let mut defaults = vec![
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        KeyValue::new(NETWORK, network),
    ];

    if let Ok(hostname) = whoami::hostname() {
        defaults.push(KeyValue::new("service.instance.id", hostname));
    }

    // A later step wins, so the environment comes last.
    Resource::builder_empty()
        .with_detector(Box::new(TelemetryResourceDetector))
        .with_service_name(config.service_name.clone())
        .with_attributes(defaults)
        .with_detector(Box::new(EnvResourceDetector::new()))
        .build()
}

/// A meter provider pushing to `exporter` every `interval`.
fn meter_provider<E>(exporter: E, interval: Duration, resource: Resource) -> SdkMeterProvider
where
    E: PushMetricExporter,
{
    let reader = PeriodicReader::builder(exporter)
        .with_interval(interval)
        .build();

    SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build()
}

/// [`meter_provider`], installed as the global one.
pub fn install_meter_provider<E>(
    exporter: E,
    interval: Duration,
    resource: Resource,
) -> SdkMeterProvider
where
    E: PushMetricExporter,
{
    let provider = meter_provider(exporter, interval, resource);
    opentelemetry::global::set_meter_provider(provider.clone());

    provider
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufRead as _, BufReader, Read as _, Write as _},
        net::TcpListener,
        sync::{mpsc, Arc},
    };

    use dolos::adapters::DomainAdapter;
    use dolos::engine::DomainBuilder;
    use dolos_core::{
        cache::{CacheEntry, CacheNamespace, CacheStore as _, FillError},
        config::RootConfig,
        ArchiveStore as _, ArchiveWriter as _, ChainPoint, Domain as _,
    };
    use opentelemetry::{
        metrics::MeterProvider as _,
        trace::{Span as _, Tracer as _, TracerProvider as _},
    };
    use opentelemetry_sdk::metrics::{
        data::{AggregatedMetrics, MetricData, ResourceMetrics},
        InMemoryMetricExporter,
    };
    use pallas::ledger::traverse::MultiEraBlock;

    use super::*;

    /// Holds the process environment for the tests that set it.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        crate::common::ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn telemetry(toml: &str) -> TelemetryConfig {
        toml::from_str(toml).unwrap()
    }

    fn preview_config(dir: &std::path::Path, extra: &str) -> RootConfig {
        let config = format!(
            r#"
            compatibility = "v2-alpha.2"

            [chain]
            protocol = "cardano"
            network = "preview"

            [upstream]
            peer_address = "unused.example:3001"

            [storage]
            path = {}

            {extra}
            "#,
            toml::Value::String(dir.join("data").display().to_string()),
        );

        toml::from_str(&config).unwrap()
    }

    fn preview_domain(config: &RootConfig) -> DomainAdapter {
        dolos::storage::ensure_storage_path(config).unwrap();

        DomainBuilder::new(config, Arc::new(dolos_cardano::include::preview::load()))
            .build()
            .unwrap()
    }

    /// Every metric of the `dolos-*` meters, by name.
    fn dolos_metrics(
        resources: &[ResourceMetrics],
    ) -> Vec<&opentelemetry_sdk::metrics::data::Metric> {
        resources
            .iter()
            .flat_map(|resource| resource.scope_metrics())
            .filter(|scope| scope.scope().name().starts_with("dolos-"))
            .flat_map(|scope| scope.metrics())
            .collect()
    }

    fn gauge(metrics: &[&opentelemetry_sdk::metrics::data::Metric], name: &str) -> Option<u64> {
        let metric = metrics.iter().find(|metric| metric.name() == name)?;

        let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data() else {
            panic!("{name} is not a u64 gauge");
        };

        gauge.data_points().next().map(|point| point.value())
    }

    /// The provider the setup path builds exports what the domain records
    /// under the new names, and nothing under the old bare ones.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_setup_provider_exports_the_cache_and_the_chain_tip() {
        let dir = tempfile::tempdir().unwrap();
        let config = preview_config(dir.path(), "[telemetry]");
        let telemetry = config.telemetry.clone().unwrap();

        let exporter = InMemoryMetricExporter::default();
        let provider = install_meter_provider(
            exporter.clone(),
            telemetry.metrics_interval(),
            resource(&telemetry, "preview".into()),
        );

        let domain = preview_domain(&config);

        let (blocks, _, _) = dolos_testing::synthetic::build_synthetic_blocks(Default::default());
        let tip = blocks.last().unwrap().clone();
        let block = MultiEraBlock::decode(&tip).unwrap();
        let writer = domain.archive().start_writer().unwrap();
        writer
            .apply(&ChainPoint::Specific(block.slot(), block.hash()), &tip)
            .unwrap();
        writer.commit().unwrap();

        domain
            .cache()
            .register(CacheNamespace::immutable("telemetry-test", 1))
            .unwrap();
        domain
            .cache()
            .get_or_fill("telemetry-test", b"key", || {
                Ok::<_, FillError>(CacheEntry::new("value", 1))
            })
            .await
            .unwrap();

        provider.force_flush().unwrap();
        let resources = exporter.get_finished_metrics().unwrap();
        let metrics = dolos_metrics(&resources);

        for metric in &metrics {
            assert!(metric.name().starts_with("dolos."), "{}", metric.name());
        }

        assert!(metrics
            .iter()
            .any(|metric| metric.name() == "dolos.cache.fills"));
        assert_eq!(gauge(&metrics, "dolos.sync.tip.slot"), Some(block.slot()));
        assert_eq!(
            gauge(&metrics, "dolos.sync.tip.block_number"),
            Some(block.number())
        );

        let fill = metrics
            .iter()
            .find(|metric| metric.name() == "dolos.cache.fill.duration")
            .expect("a fill duration");
        let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = fill.data() else {
            panic!("the fill duration is not a histogram");
        };
        let point = histogram.data_points().next().unwrap();
        let below_5s: u64 = point
            .bounds()
            .zip(point.bucket_counts())
            .filter(|(bound, _)| *bound <= 5.0)
            .map(|(_, count)| count)
            .sum();
        assert_eq!(point.count(), 1);
        assert_eq!(below_5s, 1, "the fill took seconds, not milliseconds");

        domain.shutdown().unwrap();
        provider.shutdown().unwrap();
    }

    #[test]
    fn no_telemetry_section_starts_no_exporter() {
        assert!(exporters(None).unwrap().is_none());
    }

    #[tokio::test]
    async fn metrics_off_keeps_the_span_exporter_alone() {
        let _env = env_lock();
        let config = telemetry("metrics = false");

        let exporters = exporters(Some(&config)).unwrap().unwrap();

        assert!(exporters.metrics.is_none());
        assert!(format!("{:?}", exporters.spans).contains("Tonic("));
    }

    #[test]
    fn http_protobuf_builds_http_exporters() {
        let _env = env_lock();
        let config = telemetry(
            r#"
            otlp_endpoint = "http://localhost:4318"
            otlp_protocol = "http-protobuf"
            "#,
        );

        let exporters = exporters(Some(&config)).unwrap().unwrap();

        assert!(format!("{:?}", exporters.spans).contains("Http("));
        assert!(exporters.metrics.is_some());
    }

    #[tokio::test]
    async fn an_https_grpc_endpoint_builds_with_tls() {
        let _env = env_lock();
        let config = telemetry(r#"otlp_endpoint = "https://otlp.example.com:4317""#);

        let exporters = exporters(Some(&config)).unwrap().unwrap();

        assert!(exporters.metrics.is_some());
    }

    /// One request a collector received: its path and its test header.
    type Received = (String, Option<String>);

    /// Answers every OTLP/HTTP request with 200 and reports what it got.
    fn collector() -> (String, mpsc::Receiver<Received>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, received) = mpsc::channel();

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut reader = BufReader::new(stream.try_clone().unwrap());

                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                let path = request_line
                    .split(' ')
                    .nth(1)
                    .unwrap_or_default()
                    .to_string();

                let (mut length, mut header) = (0, None);
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    let line = line.trim_end();
                    if line.is_empty() {
                        break;
                    }
                    let (name, value) = line.split_once(": ").unwrap_or((line, ""));
                    match name.to_ascii_lowercase().as_str() {
                        "content-length" => length = value.parse().unwrap(),
                        "x-dolos-test" => header = Some(value.to_string()),
                        _ => (),
                    }
                }

                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                    .unwrap();

                if sender.send((path, header)).is_err() {
                    return;
                }
            }
        });

        (format!("http://{address}"), received)
    }

    #[test]
    fn http_protobuf_posts_each_signal_under_the_base_url_with_the_env_headers() {
        let (address, received) = collector();
        let config = telemetry(&format!(
            r#"
            otlp_endpoint = "{address}/otlp/"
            otlp_protocol = "http-protobuf"
            "#
        ));

        let exporters = {
            let _env = env_lock();
            std::env::set_var("OTEL_EXPORTER_OTLP_HEADERS", "x-dolos-test=from-env");
            let exporters = exporters(Some(&config));
            std::env::remove_var("OTEL_EXPORTER_OTLP_HEADERS");
            exporters.unwrap().unwrap()
        };

        let tracer = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(exporters.spans)
            .build();
        tracer.tracer("test").start("work").end();

        let meters = meter_provider(
            exporters.metrics.unwrap(),
            Duration::from_secs(3600),
            Resource::builder_empty().build(),
        );
        meters.meter("test").u64_counter("work").build().add(1, &[]);
        meters.force_flush().unwrap();

        let mut got: Vec<Received> = (0..2)
            .map(|_| received.recv_timeout(Duration::from_secs(10)).unwrap())
            .collect();
        got.sort();

        let header = Some("from-env".to_string());
        assert_eq!(
            got,
            [
                ("/otlp/v1/metrics".to_string(), header.clone()),
                ("/otlp/v1/traces".to_string(), header),
            ]
        );

        tracer.shutdown().unwrap();
        meters.shutdown().unwrap();
    }

    fn attribute(resource: &Resource, key: &'static str) -> Option<String> {
        resource
            .get(&opentelemetry::Key::from_static_str(key))
            .map(|value| value.to_string())
    }

    #[test]
    fn the_resource_names_the_process_and_the_environment_overrides_it() {
        let _env = env_lock();
        let config = telemetry(r#"service_name = "dolos-preview""#);

        std::env::remove_var("OTEL_RESOURCE_ATTRIBUTES");
        let defaults = resource(&config, "preview".into());

        assert_eq!(
            attribute(&defaults, "service.name").as_deref(),
            Some("dolos-preview")
        );
        assert_eq!(
            attribute(&defaults, "service.version").as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(attribute(&defaults, NETWORK).as_deref(), Some("preview"));
        assert_eq!(
            attribute(&defaults, "service.instance.id"),
            whoami::hostname().ok()
        );

        std::env::set_var(
            "OTEL_RESOURCE_ATTRIBUTES",
            "service.instance.id=dolos-0,dolos.network=42",
        );
        let overridden = resource(&config, "preview".into());
        std::env::remove_var("OTEL_RESOURCE_ATTRIBUTES");

        assert_eq!(
            attribute(&overridden, "service.instance.id").as_deref(),
            Some("dolos-0")
        );
        assert_eq!(attribute(&overridden, NETWORK).as_deref(), Some("42"));
        assert_eq!(
            attribute(&overridden, "service.name").as_deref(),
            Some("dolos-preview")
        );
    }
}
