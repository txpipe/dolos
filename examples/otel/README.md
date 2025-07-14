# OTLP Exporter Example

This example demonstrates how to set up an OpenTelemetry OTLP exporter for logs
and metrics for Dolos, to send data to the [OpenTelemetry
Collector](https://github.com/open-telemetry/opentelemetry-collector) via OTLP
over HTTP. The Collector then forwards the data to the configured backend,
which in this case is the logging exporter, displaying data on the console, and
prometheus exporter for the metrics, setting up a prometheus server on port
8889.

## Usage

Run the `otel/opentelemetry-collector` container using docker
and inspect the logs to see the exported telemetry.

On Unix based systems use:

```shell
# From the current directory, run `opentelemetry-collector`
docker run --rm -it -p 4317:4317 -p 4318:4318 -p 8889:8889 -v $(pwd):/cfg otel/opentelemetry-collector:latest --config=/cfg/otel-collector-config.yml
```

On Windows use:

```shell
# From the current directory, run `opentelemetry-collector`
docker run --rm -it -p 4317:4317 -p 4318:4318 -p 8889:8889 -v "%cd%":/cfg otel/opentelemetry-collector:latest --config=/cfg/otel-collector-config.yml
```

Run Dolos which exports logs and metrics via OTLP to the collector:

```shell
cargo run --release --bin dolos daemon
```

Both the logging and metrics will be visible via STDOUT on the Open Telemetry collector, but you can also see the prometheus metrics doing:

```shell
curl http://localhost:8889/metrics

```
