//! Request metrics of the TRP server: the core RPC instruments, labelled
//! with the JSON-RPC method and the response's status.

use std::future::Future;

use dolos_core::telemetry::requests::{Family, RequestMetrics};
use opentelemetry::{global, metrics::Meter, KeyValue};

use crate::Error;

/// The meter of the TRP instruments.
pub const METER: &str = "dolos-trp";

const SURFACE: &str = "trp";
const RPC_METHOD: &str = "rpc.method";
const RPC_STATUS_CODE: &str = "rpc.response.status_code";

#[derive(Clone)]
pub struct Metrics {
    requests: RequestMetrics,
}

impl Metrics {
    /// The instruments on the global [`METER`].
    pub fn new() -> Self {
        Self::with_meter(&global::meter(METER))
    }

    /// The same instruments on `meter`.
    pub fn with_meter(meter: &Meter) -> Self {
        Self {
            requests: RequestMetrics::with_meter(Family::Rpc, meter, SURFACE),
        }
    }

    /// Wraps the handler of `method`. The call is active from now until the
    /// returned future completes or is dropped; completing records its
    /// duration with `OK` or the JSON-RPC error code.
    pub fn track<T, H>(&self, method: &'static str, handler: H) -> impl Future<Output = H::Output>
    where
        H: Future<Output = Result<T, Error>>,
    {
        let guard = self.requests.start([KeyValue::new(RPC_METHOD, method)]);

        async move {
            let response = handler.await;

            let status = match &response {
                Ok(_) => "OK".to_string(),
                Err(err) => err.code().to_string(),
            };

            guard.finish([KeyValue::new(RPC_STATUS_CODE, status)]);

            response
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use jsonrpsee::types::Params;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{
        data::{AggregatedMetrics, MetricData},
        InMemoryMetricExporter, SdkMeterProvider,
    };
    use serde_json::{json, Value};

    use super::*;
    use crate::{methods, methods::tests::setup_test_context_with_metrics, rpc_module};

    struct Harness {
        provider: SdkMeterProvider,
        exporter: InMemoryMetricExporter,
        context: Arc<crate::Context<dolos_testing::toy_domain::ToyDomain>>,
    }

    impl Harness {
        async fn new() -> Self {
            let exporter = InMemoryMetricExporter::default();
            let provider = SdkMeterProvider::builder()
                .with_periodic_exporter(exporter.clone())
                .build();
            let metrics = Metrics::with_meter(&provider.meter(METER));
            let context = setup_test_context_with_metrics(metrics).await;

            Self {
                provider,
                exporter,
                context,
            }
        }

        /// Sends one raw JSON-RPC request through the registered module.
        async fn call(&self, method: &str, params: Value) -> Value {
            let module = rpc_module(self.context.as_ref().clone()).unwrap();
            let request = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });

            let (response, _) = module
                .raw_json_request(&request.to_string(), 1)
                .await
                .unwrap();

            serde_json::from_str(&response).unwrap()
        }

        /// Every exported point as `name{attributes} value`, sorted.
        fn collect(&self) -> Vec<String> {
            self.exporter.reset();
            self.provider.force_flush().unwrap();

            let mut points = Vec::new();

            for resource in self.exporter.get_finished_metrics().unwrap() {
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
                                    push(
                                        labels(&mut point.attributes()),
                                        point.value().to_string(),
                                    );
                                }
                            }
                            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                                for point in sum.data_points() {
                                    push(
                                        labels(&mut point.attributes()),
                                        point.value().to_string(),
                                    );
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
    }

    #[tokio::test]
    async fn a_successful_call_records_its_method_and_ok() {
        let harness = Harness::new().await;
        let hash = hex::encode([0xABu8; 32]);

        let response = harness
            .call("trp.checkStatus", json!({ "hashes": [hash] }))
            .await;

        assert_eq!(response["result"]["statuses"][&hash]["stage"], "unknown");
        assert_eq!(
            harness.collect(),
            [
                "dolos.rpc.server.active_requests{rpc.method=trp.checkStatus,surface=trp} 0",
                "rpc.server.call.duration{rpc.method=trp.checkStatus,rpc.response.status_code=OK,surface=trp} count=1",
            ]
        );
    }

    #[tokio::test]
    async fn a_failing_call_records_its_error_code() {
        let harness = Harness::new().await;

        let response = harness
            .call("trp.checkStatus", json!({ "hashes": 5 }))
            .await;

        assert_eq!(response["error"]["code"], -32602);
        assert_eq!(
            harness.collect(),
            [
                "dolos.rpc.server.active_requests{rpc.method=trp.checkStatus,surface=trp} 0",
                "rpc.server.call.duration{rpc.method=trp.checkStatus,rpc.response.status_code=-32602,surface=trp} count=1",
            ]
        );
    }

    #[tokio::test]
    async fn a_handler_dropped_before_its_response_counts_as_dropped() {
        let harness = Harness::new().await;
        let request = json!({ "hashes": [] }).to_string();
        let params = Params::new(Some(request.as_str())).into_owned();

        let handler = harness.context.metrics.track(
            "trp.checkStatus",
            methods::trp_check_status(params, harness.context.clone()),
        );

        assert_eq!(
            harness.collect(),
            ["dolos.rpc.server.active_requests{rpc.method=trp.checkStatus,surface=trp} 1"]
        );

        drop(handler);

        assert_eq!(
            harness.collect(),
            [
                "dolos.rpc.server.active_requests{rpc.method=trp.checkStatus,surface=trp} 0",
                "dolos.rpc.server.request.dropped{rpc.method=trp.checkStatus,surface=trp} 1",
            ]
        );
    }

    #[tokio::test]
    async fn health_and_unknown_methods_are_not_recorded() {
        let harness = Harness::new().await;

        let health = harness.call("health", json!([])).await;
        let unknown = harness.call("trp.nope", json!([])).await;

        assert_eq!(health["result"], true);
        assert_eq!(unknown["error"]["code"], -32601);
        assert!(harness.collect().is_empty(), "{:?}", harness.collect());
    }

    #[tokio::test]
    async fn no_instrument_named_requests_is_exported() {
        let harness = Harness::new().await;

        harness.call("trp.peekPending", json!({})).await;
        harness
            .call("trp.checkStatus", json!({ "hashes": 5 }))
            .await;

        let points = harness.collect();

        assert!(!points.is_empty());
        assert!(
            points.iter().all(|point| !point.starts_with("requests{")),
            "{points:?}"
        );
    }

    /// The result, or the error code, of one call.
    fn outcome_of<T: serde::Serialize>(response: Result<T, Error>) -> Value {
        match response {
            Ok(result) => json!({ "result": result }),
            Err(err) => json!({ "error": err.code() }),
        }
    }

    /// The same outcome, read from a JSON-RPC response.
    fn outcome(response: &Value) -> Value {
        match response.get("result") {
            Some(result) => json!({ "result": result }),
            None => json!({ "error": response["error"]["code"] }),
        }
    }

    /// Calls the handler of `method` directly, outside the module.
    async fn call_handler(
        method: &str,
        params: &Value,
        context: Arc<crate::Context<dolos_testing::toy_domain::ToyDomain>>,
    ) -> Value {
        let raw = params.to_string();
        let params = Params::new(Some(raw.as_str()));

        match method {
            "trp.resolve" => outcome_of(methods::trp_resolve(params, context).await),
            "trp.submit" => outcome_of(methods::trp_submit(params, context).await),
            "trp.checkStatus" => outcome_of(methods::trp_check_status(params, context).await),
            "trp.dumpLogs" => outcome_of(methods::trp_dump_logs(params, context).await),
            "trp.peekPending" => outcome_of(methods::trp_peek_pending(params, context).await),
            "trp.peekInflight" => outcome_of(methods::trp_peek_inflight(params, context).await),
            _ => unreachable!("{method}"),
        }
    }

    #[tokio::test]
    async fn every_method_is_served_by_its_handler_and_recorded_under_its_name() {
        let harness = Harness::new().await;

        let pending = dolos_testing::tx_sequence_to_hash(0);
        dolos_core::MempoolStore::receive(
            dolos_core::Domain::mempool(&harness.context.domain),
            dolos_testing::mempool::make_test_mempool_tx(pending),
        )
        .unwrap();

        let cases = [
            (
                "trp.resolve",
                json!({
                    "tir": {
                        "version": "v1beta0",
                        "content": methods::tests::SUBJECT_PROTOCOL,
                        "encoding": "hex"
                    },
                    "args": {},
                    "env": {},
                }),
            ),
            (
                "trp.submit",
                json!({ "tx": { "content": "00", "contentType": "hex" }, "witnesses": [] }),
            ),
            (
                "trp.checkStatus",
                json!({ "hashes": [hex::encode(pending.as_ref())] }),
            ),
            ("trp.dumpLogs", json!({})),
            ("trp.peekPending", json!({})),
            // The test mempool has no inflight or finalized transactions, so
            // peekInflight and dumpLogs both answer an empty list. A cursor
            // tells them apart: peekInflight ignores it, dumpLogs rejects one
            // that is not a number.
            ("trp.peekInflight", json!({ "cursor": "unused" })),
        ];

        for (method, params) in &cases {
            let expected = call_handler(method, params, harness.context.clone()).await;
            let response = harness.call(method, params.clone()).await;

            assert_eq!(outcome(&response), expected, "{method}: {response}");
        }

        let points = harness.collect();
        let durations: Vec<_> = points
            .iter()
            .filter(|point| point.starts_with("rpc.server.call.duration{"))
            .collect();

        assert_eq!(durations.len(), cases.len(), "{points:?}");

        for (method, _) in &cases {
            assert!(
                durations.iter().any(|point| {
                    point.contains(&format!("{{rpc.method={method},"))
                        && point.ends_with(" count=1")
                }),
                "{method}: {points:?}"
            );
        }
    }
}
