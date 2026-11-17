//! Request metrics of the gRPC server.
//!
//! [`MetricsLayer`] wraps the server's services. It times every gRPC and
//! gRPC-web call with the shared RPC request instruments, labelled by service
//! and method, and tracks how many server streams are open and how they end.
//! Requests of any other kind, such as CORS preflights, pass through
//! unrecorded.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use base64::Engine as _;
use dolos_core::telemetry::requests::{Family, RequestGuard, RequestMetrics};
use dolos_core::telemetry::SURFACE;
use http::{header::CONTENT_TYPE, HeaderMap, Request, Response};
use http_body::{Body, Frame, SizeHint};
use opentelemetry::{
    global,
    metrics::{Counter, Meter, UpDownCounter},
    KeyValue, StringValue,
};
use prost::Message as _;
use tower_layer::Layer;
use tower_service::Service;

/// The meter of the gRPC server's instruments.
pub const METER: &str = "dolos-grpc";

/// The `surface` label of the gRPC server's samples.
const SURFACE_NAME: &str = "grpc";

const RPC_SERVICE: &str = "rpc.service";
const RPC_METHOD: &str = "rpc.method";
const RPC_STATUS_CODE: &str = "rpc.response.status_code";

/// The label of a path that names no registered method.
const UNKNOWN_METHOD: &str = "unknown";

/// gRPC status names, indexed by code.
const STATUS_NAMES: [&str; 17] = [
    "OK",
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
];

const OK: &str = STATUS_NAMES[0];
const CANCELLED: &str = STATUS_NAMES[1];
const UNKNOWN: &str = STATUS_NAMES[2];

/// The name of the status a `grpc-status` value carries; codes outside the
/// gRPC set are `UNKNOWN`.
fn status_name(value: &[u8]) -> &'static str {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .and_then(|code| STATUS_NAMES.get(code).copied())
        .unwrap_or(UNKNOWN)
}

fn header_status(headers: &HeaderMap) -> Option<&'static str> {
    headers
        .get("grpc-status")
        .map(|value| status_name(value.as_bytes()))
}

/// The `grpc-status` of a gRPC-web trailer block, `name:value` lines
/// separated by CRLF.
fn block_status(block: &[u8]) -> Option<&'static str> {
    block.split(|b| *b == b'\n').find_map(|line| {
        let colon = line.iter().position(|b| *b == b':')?;
        let (name, value) = line.split_at(colon);

        name.trim_ascii()
            .eq_ignore_ascii_case(b"grpc-status")
            .then(|| status_name(value[1..].trim_ascii()))
    })
}

#[derive(Clone)]
struct Method {
    labels: [KeyValue; 2],
    streaming: bool,
}

impl Method {
    fn new(service: &str, method: &str, streaming: bool) -> Self {
        let label = |key: &'static str, value: &str| {
            KeyValue::new(key, StringValue::from(Arc::<str>::from(value)))
        };

        Self {
            labels: [label(RPC_SERVICE, service), label(RPC_METHOD, method)],
            streaming,
        }
    }
}

/// The methods the server registers, by request path (`/pkg.Service/Method`).
pub struct Methods {
    by_path: HashMap<String, Method>,
    unknown: Method,
}

impl Methods {
    /// The methods of every service declared in the encoded file descriptor
    /// sets.
    ///
    /// # Panics
    ///
    /// If a set does not decode: the sets are compiled into the binary.
    pub fn from_descriptor_sets(sets: &[&[u8]]) -> Self {
        let mut by_path = HashMap::new();

        for set in sets {
            let set = prost_types::FileDescriptorSet::decode(*set)
                .expect("compiled file descriptor sets decode");

            for file in set.file {
                for service in &file.service {
                    let service_name = match file.package() {
                        "" => service.name().to_string(),
                        package => format!("{package}.{}", service.name()),
                    };

                    for method in &service.method {
                        by_path.insert(
                            format!("/{service_name}/{}", method.name()),
                            Method::new(&service_name, method.name(), method.server_streaming()),
                        );
                    }
                }
            }
        }

        Self {
            by_path,
            unknown: Method::new(UNKNOWN_METHOD, UNKNOWN_METHOD, false),
        }
    }

    fn get(&self, path: &str) -> &Method {
        self.by_path.get(path).unwrap_or(&self.unknown)
    }
}

struct Shared {
    methods: Methods,
    requests: RequestMetrics,
    active_streams: UpDownCounter<i64>,
    streams_ended: Counter<u64>,
    surface: KeyValue,
}

/// The tower layer that records the gRPC server's metrics.
#[derive(Clone)]
pub struct MetricsLayer(Arc<Shared>);

impl MetricsLayer {
    /// The layer on the global [`METER`].
    pub fn new(methods: Methods) -> Self {
        Self::with_meter(&global::meter(METER), methods)
    }

    /// The layer on `meter`.
    pub fn with_meter(meter: &Meter, methods: Methods) -> Self {
        Self(Arc::new(Shared {
            methods,
            requests: RequestMetrics::with_meter(Family::Rpc, meter, SURFACE_NAME),
            active_streams: meter
                .i64_up_down_counter("dolos.rpc.server.active_streams")
                .with_unit("{stream}")
                .build(),
            streams_ended: meter
                .u64_counter("dolos.rpc.server.streams.ended")
                .with_unit("{stream}")
                .build(),
            surface: KeyValue::new(SURFACE, SURFACE_NAME),
        }))
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            shared: self.0.clone(),
        }
    }
}

/// The service [`MetricsLayer`] wraps around the server's.
#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    shared: Arc<Shared>,
}

fn is_grpc(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/grpc"))
}

/// A gRPC call in flight, from its request to the end of its response.
struct Call {
    shared: Arc<Shared>,
    method: Method,
    guard: Option<RequestGuard>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for MetricsService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = Response<MetricsBody<ResBody>>;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let call = is_grpc(request.headers()).then(|| {
            let method = self.shared.methods.get(request.uri().path()).clone();
            let guard = self.shared.requests.start(method.labels.clone());

            Call {
                shared: self.shared.clone(),
                method,
                guard: Some(guard),
            }
        });

        ResponseFuture {
            inner: self.inner.call(request),
            call,
        }
    }
}

pin_project_lite::pin_project! {
    /// The future of a [`MetricsService`] response. Dropping it before the
    /// response counts the call as dropped.
    pub struct ResponseFuture<F> {
        #[pin]
        inner: F,
        call: Option<Call>,
    }
}

impl<F, B, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<Response<B>, E>>,
{
    type Output = Result<Response<MetricsBody<B>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.inner.poll(cx))?;

        let tracker = this.call.take().map(|call| Tracker::new(call, &response));

        Poll::Ready(Ok(response.map(|inner| MetricsBody { inner, tracker })))
    }
}

/// Reads the length-prefixed frames of a gRPC or gRPC-web body: messages,
/// and the gRPC-web trailer frame (flag bit `0x80`).
#[derive(Default)]
struct FrameReader {
    header: [u8; 5],
    header_len: usize,
    remaining: usize,
    trailer: Option<Vec<u8>>,
}

#[derive(Default)]
struct Seen {
    message: bool,
    trailer: Option<Vec<u8>>,
}

impl FrameReader {
    fn feed(&mut self, mut data: &[u8]) -> Seen {
        let mut seen = Seen::default();

        while !data.is_empty() {
            if self.remaining > 0 {
                let take = self.remaining.min(data.len());

                if let Some(trailer) = &mut self.trailer {
                    trailer.extend_from_slice(&data[..take]);
                }

                self.remaining -= take;
                data = &data[take..];
            } else {
                let take = (self.header.len() - self.header_len).min(data.len());
                self.header[self.header_len..self.header_len + take].copy_from_slice(&data[..take]);
                self.header_len += take;
                data = &data[take..];

                if self.header_len < self.header.len() {
                    continue;
                }

                self.header_len = 0;
                self.remaining = u32::from_be_bytes(self.header[1..].try_into().unwrap()) as usize;

                if self.header[0] & 0x80 != 0 {
                    self.trailer = Some(Vec::new());
                } else {
                    seen.message = true;
                }
            }

            if self.remaining == 0 && self.header_len == 0 {
                if let Some(trailer) = self.trailer.take() {
                    seen.trailer = Some(trailer);
                }
            }
        }

        seen
    }
}

/// What a response's body has shown so far, and what is left to record.
struct Tracker {
    call: Call,
    /// The status from the response headers or trailers, once known.
    status: Option<&'static str>,
    /// Whether the body is base64 (`application/grpc-web-text`).
    text: bool,
    /// `None` once a chunk could not be read, so later bytes are not misread.
    frames: Option<FrameReader>,
    stream_open: bool,
}

impl Tracker {
    fn new<B>(call: Call, response: &Response<B>) -> Self {
        let headers = response.headers();
        let text = headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/grpc-web-text"));

        let stream_open = call.method.streaming;
        if stream_open {
            let labels = Self::stream_labels(&call);
            call.shared.active_streams.add(1, &labels);
        }

        Self {
            status: header_status(headers),
            text,
            frames: Some(FrameReader::default()),
            stream_open,
            call,
        }
    }

    fn stream_labels(call: &Call) -> [KeyValue; 3] {
        let [service, method] = call.method.labels.clone();
        [service, method, call.shared.surface.clone()]
    }

    fn on_data(&mut self, data: &[u8]) {
        let Some(frames) = &mut self.frames else {
            return;
        };

        let seen = if self.text {
            match base64::engine::general_purpose::STANDARD.decode(data) {
                Ok(decoded) => frames.feed(&decoded),
                Err(_) => {
                    self.frames = None;
                    return;
                }
            }
        } else {
            frames.feed(data)
        };

        if seen.message && self.call.method.streaming {
            if let Some(guard) = self.call.guard.take() {
                guard.finish([KeyValue::new(RPC_STATUS_CODE, OK)]);
            }
        }

        if let Some(status) = seen.trailer.as_deref().and_then(block_status) {
            self.status = Some(status);
            self.end();
        }
    }

    fn on_trailers(&mut self, trailers: &HeaderMap) {
        if let Some(status) = header_status(trailers) {
            self.status = Some(status);
        }

        self.end();
    }

    /// Records the end of the response with its status; later calls do nothing.
    fn end(&mut self) {
        let status = self.status.unwrap_or(UNKNOWN);

        if let Some(guard) = self.call.guard.take() {
            guard.finish([KeyValue::new(RPC_STATUS_CODE, status)]);
        }

        self.close_stream(status);
    }

    fn close_stream(&mut self, status: &'static str) {
        if !std::mem::take(&mut self.stream_open) {
            return;
        }

        let labels = Self::stream_labels(&self.call);
        self.call.shared.active_streams.add(-1, &labels);

        let [service, method, surface] = labels;
        self.call.shared.streams_ended.add(
            1,
            &[
                service,
                method,
                surface,
                KeyValue::new(RPC_STATUS_CODE, status),
            ],
        );
    }
}

impl Drop for Tracker {
    fn drop(&mut self) {
        // A status already seen (a trailers-only response the server never
        // polled) ends the call; otherwise the client went away first.
        if self.status.is_some() {
            self.end();
        } else {
            drop(self.call.guard.take());
            self.close_stream(CANCELLED);
        }
    }
}

pin_project_lite::pin_project! {
    /// A response body that records the call's end as it is read.
    pub struct MetricsBody<B> {
        #[pin]
        inner: B,
        tracker: Option<Tracker>,
    }
}

impl<B> Body for MetricsBody<B>
where
    B: Body,
    B::Data: AsRef<[u8]>,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let frame = ready!(this.inner.poll_frame(cx));

        if let Some(tracker) = this.tracker {
            match &frame {
                Some(Ok(frame)) => {
                    if let Some(data) = frame.data_ref() {
                        tracker.on_data(data.as_ref());
                    } else if let Some(trailers) = frame.trailers_ref() {
                        tracker.on_trailers(trailers);
                    }
                }
                None => tracker.end(),
                Some(Err(_)) => {}
            }
        }

        Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, convert::Infallible};

    use bytes::Bytes;
    use http::{HeaderValue, Method as HttpMethod};
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{
        data::{AggregatedMetrics, MetricData},
        InMemoryMetricExporter, SdkMeterProvider,
    };
    use tower::ServiceExt as _;

    use super::*;

    const UNARY: &str = "/utxorpc.v1alpha.sync.SyncService/ReadTip";
    const STREAM: &str = "/utxorpc.v1alpha.sync.SyncService/FollowTip";

    /// A body that yields its frames in order.
    #[derive(Default)]
    struct TestBody(VecDeque<Frame<Bytes>>);

    impl Body for TestBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            Poll::Ready(self.0.pop_front().map(Ok))
        }

        fn is_end_stream(&self) -> bool {
            self.0.is_empty()
        }
    }

    fn message(payload: &[u8]) -> Bytes {
        let mut frame = vec![0];
        frame.extend((payload.len() as u32).to_be_bytes());
        frame.extend(payload);
        frame.into()
    }

    fn web_trailer(code: u8) -> Bytes {
        let block = format!("grpc-status:{code}\r\ngrpc-message:done\r\n");
        let mut frame = vec![0x80];
        frame.extend((block.len() as u32).to_be_bytes());
        frame.extend(block.as_bytes());
        frame.into()
    }

    fn trailers(code: u8) -> Frame<Bytes> {
        let mut map = HeaderMap::new();
        map.insert("grpc-status", HeaderValue::from(u16::from(code)));
        Frame::trailers(map)
    }

    /// A stub server answering every request with `headers` and `frames`.
    fn stub(
        headers: Vec<(&'static str, &'static str)>,
        frames: Vec<Frame<Bytes>>,
    ) -> impl Service<
        Request<()>,
        Response = Response<TestBody>,
        Error = Infallible,
        Future = std::future::Ready<Result<Response<TestBody>, Infallible>>,
    > + Clone {
        let frames = Arc::new(frames);

        tower::service_fn(move |_| {
            let mut response = Response::new(TestBody(frames.iter().map(clone_frame).collect()));
            for (name, value) in &headers {
                response
                    .headers_mut()
                    .insert(*name, HeaderValue::from_static(value));
            }
            std::future::ready(Ok(response))
        })
    }

    fn clone_frame(frame: &Frame<Bytes>) -> Frame<Bytes> {
        match (frame.data_ref(), frame.trailers_ref()) {
            (Some(data), _) => Frame::data(data.clone()),
            (_, Some(trailers)) => Frame::trailers(trailers.clone()),
            _ => unreachable!(),
        }
    }

    fn request(path: &str, content_type: &'static str) -> Request<()> {
        Request::builder()
            .method(HttpMethod::POST)
            .uri(path)
            .header(CONTENT_TYPE, content_type)
            .body(())
            .unwrap()
    }

    fn setup() -> (SdkMeterProvider, InMemoryMetricExporter, MetricsLayer) {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let methods = super::super::registered_methods();
        let layer = MetricsLayer::with_meter(&provider.meter("test"), methods);

        (provider, exporter, layer)
    }

    /// Every exported point as `name{attributes} value`, sorted.
    fn collect(provider: &SdkMeterProvider, exporter: &InMemoryMetricExporter) -> Vec<String> {
        exporter.reset();
        provider.force_flush().unwrap();

        let mut points = Vec::new();

        for resource in exporter.get_finished_metrics().unwrap() {
            for scope in resource.scope_metrics() {
                for metric in scope.metrics() {
                    let mut push = |attributes: &mut dyn Iterator<Item = &KeyValue>, value| {
                        let mut attributes: Vec<_> = attributes
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
                        _ => panic!("unexpected instrument {}", metric.name()),
                    }
                }
            }
        }

        points.sort();
        points
    }

    /// One exported point of a `SyncService` method, as [`collect`] prints it.
    fn point(name: &str, method: &str, status: Option<&str>, value: &str) -> String {
        let mut attributes = vec![
            format!("rpc.method={method}"),
            "rpc.service=utxorpc.v1alpha.sync.SyncService".to_string(),
            "surface=grpc".to_string(),
        ];
        attributes.extend(status.map(|status| format!("rpc.response.status_code={status}")));
        attributes.sort();

        format!("{name}{{{}}} {value}", attributes.join(","))
    }

    async fn read_to_end<B: Body + Unpin>(body: &mut B)
    where
        B::Error: std::fmt::Debug,
    {
        use http_body_util::BodyExt as _;

        while let Some(frame) = body.frame().await {
            frame.unwrap();
        }
    }

    async fn next_frame<B: Body + Unpin>(body: &mut B)
    where
        B::Error: std::fmt::Debug,
    {
        use http_body_util::BodyExt as _;

        body.frame().await.unwrap().unwrap();
    }

    #[test]
    fn the_registered_methods_come_from_the_descriptor_sets() {
        let methods = super::super::registered_methods();

        assert!(!methods.get(UNARY).streaming);
        assert!(methods.get(STREAM).streaming);
        assert!(
            methods
                .get("/utxorpc.v1beta.watch.WatchService/WatchTx")
                .streaming
        );
        assert!(
            methods
                .get("/utxorpc.v1beta.submit.SubmitService/WaitForTx")
                .streaming
        );
        assert!(
            !methods
                .get("/utxorpc.v1beta.query.QueryService/ReadParams")
                .streaming
        );
        assert!(
            methods
                .get("/grpc.reflection.v1.ServerReflection/ServerReflectionInfo")
                .streaming
        );
        assert_eq!(
            methods
                .get("/utxorpc.v1beta.query.QueryService/ReadParams")
                .labels[0]
                .value
                .as_str(),
            "utxorpc.v1beta.query.QueryService"
        );
    }

    #[tokio::test]
    async fn a_unary_success_records_one_ok_duration() {
        let (provider, exporter, layer) = setup();
        let service = layer.layer(stub(
            vec![("content-type", "application/grpc")],
            vec![Frame::data(message(b"tip")), trailers(0)],
        ));

        let mut body = service
            .oneshot(request(UNARY, "application/grpc"))
            .await
            .unwrap()
            .into_body();
        read_to_end(&mut body).await;

        assert_eq!(
            collect(&provider, &exporter),
            [
                point("dolos.rpc.server.active_requests", "ReadTip", None, "0"),
                point("rpc.server.call.duration", "ReadTip", Some("OK"), "count=1"),
            ]
        );
    }

    #[tokio::test]
    async fn a_unary_error_in_the_headers_records_its_code() {
        let (provider, exporter, layer) = setup();
        let service = layer.layer(stub(
            vec![("content-type", "application/grpc"), ("grpc-status", "5")],
            vec![],
        ));

        // A trailers-only response: the server sends the headers and drops
        // the empty body without polling it.
        let response = service
            .oneshot(request(UNARY, "application/grpc"))
            .await
            .unwrap();
        assert!(response.body().is_end_stream());
        drop(response);

        assert_eq!(
            collect(&provider, &exporter),
            [
                point("dolos.rpc.server.active_requests", "ReadTip", None, "0"),
                point(
                    "rpc.server.call.duration",
                    "ReadTip",
                    Some("NOT_FOUND"),
                    "count=1"
                ),
            ]
        );
    }

    #[tokio::test]
    async fn a_unary_error_in_the_trailers_records_its_code() {
        let (provider, exporter, layer) = setup();
        let service = layer.layer(stub(
            vec![("content-type", "application/grpc")],
            vec![trailers(13)],
        ));

        let mut body = service
            .oneshot(request(UNARY, "application/grpc"))
            .await
            .unwrap()
            .into_body();
        read_to_end(&mut body).await;

        assert_eq!(
            collect(&provider, &exporter),
            [
                point("dolos.rpc.server.active_requests", "ReadTip", None, "0"),
                point(
                    "rpc.server.call.duration",
                    "ReadTip",
                    Some("INTERNAL"),
                    "count=1"
                ),
            ]
        );
    }

    #[tokio::test]
    async fn a_unary_call_dropped_before_its_response_counts_as_dropped() {
        let (provider, exporter, layer) = setup();
        let mut service = layer.layer(tower::service_fn(|_: Request<()>| {
            std::future::pending::<Result<Response<TestBody>, Infallible>>()
        }));

        let future = service.call(request(UNARY, "application/grpc"));

        assert_eq!(
            collect(&provider, &exporter),
            [point(
                "dolos.rpc.server.active_requests",
                "ReadTip",
                None,
                "1"
            )]
        );

        drop(future);

        assert_eq!(
            collect(&provider, &exporter),
            [
                point("dolos.rpc.server.active_requests", "ReadTip", None, "0"),
                point("dolos.rpc.server.request.dropped", "ReadTip", None, "1"),
            ]
        );
    }

    #[tokio::test]
    async fn a_stream_ended_by_the_server_records_its_duration_at_the_first_message() {
        let (provider, exporter, layer) = setup();
        let service = layer.layer(stub(
            vec![("content-type", "application/grpc")],
            vec![
                Frame::data(message(b"one")),
                Frame::data(message(b"two")),
                trailers(0),
            ],
        ));

        let mut body = service
            .oneshot(request(STREAM, "application/grpc"))
            .await
            .unwrap()
            .into_body();

        assert_eq!(
            collect(&provider, &exporter),
            [
                point("dolos.rpc.server.active_requests", "FollowTip", None, "1"),
                point("dolos.rpc.server.active_streams", "FollowTip", None, "1"),
            ]
        );

        next_frame(&mut body).await;

        assert_eq!(
            collect(&provider, &exporter),
            [
                point("dolos.rpc.server.active_requests", "FollowTip", None, "0"),
                point("dolos.rpc.server.active_streams", "FollowTip", None, "1"),
                point(
                    "rpc.server.call.duration",
                    "FollowTip",
                    Some("OK"),
                    "count=1"
                ),
            ]
        );

        read_to_end(&mut body).await;

        assert_eq!(
            collect(&provider, &exporter),
            [
                point("dolos.rpc.server.active_requests", "FollowTip", None, "0"),
                point("dolos.rpc.server.active_streams", "FollowTip", None, "0"),
                point(
                    "dolos.rpc.server.streams.ended",
                    "FollowTip",
                    Some("OK"),
                    "1"
                ),
                point(
                    "rpc.server.call.duration",
                    "FollowTip",
                    Some("OK"),
                    "count=1"
                ),
            ]
        );
    }

    #[tokio::test]
    async fn a_stream_dropped_by_the_client_ends_cancelled() {
        let (provider, exporter, layer) = setup();
        let service = layer.layer(stub(
            vec![("content-type", "application/grpc")],
            vec![Frame::data(message(b"one")), Frame::data(message(b"two"))],
        ));

        let mut body = service
            .oneshot(request(STREAM, "application/grpc"))
            .await
            .unwrap()
            .into_body();
        next_frame(&mut body).await;
        drop(body);

        assert_eq!(
            collect(&provider, &exporter),
            [
                point("dolos.rpc.server.active_requests", "FollowTip", None, "0"),
                point("dolos.rpc.server.active_streams", "FollowTip", None, "0"),
                point(
                    "dolos.rpc.server.streams.ended",
                    "FollowTip",
                    Some("CANCELLED"),
                    "1"
                ),
                point(
                    "rpc.server.call.duration",
                    "FollowTip",
                    Some("OK"),
                    "count=1"
                ),
            ]
        );
    }

    #[tokio::test]
    async fn a_stream_dropped_before_any_message_counts_as_dropped() {
        let (provider, exporter, layer) = setup();
        let service = layer.layer(stub(vec![("content-type", "application/grpc")], vec![]));

        let response = service
            .oneshot(request(STREAM, "application/grpc"))
            .await
            .unwrap();
        drop(response);

        assert_eq!(
            collect(&provider, &exporter),
            [
                point("dolos.rpc.server.active_requests", "FollowTip", None, "0"),
                point("dolos.rpc.server.active_streams", "FollowTip", None, "0"),
                point("dolos.rpc.server.request.dropped", "FollowTip", None, "1"),
                point(
                    "dolos.rpc.server.streams.ended",
                    "FollowTip",
                    Some("CANCELLED"),
                    "1"
                ),
            ]
        );
    }

    #[tokio::test]
    async fn a_grpc_web_unary_call_reports_the_status_of_its_trailer_frame() {
        let (provider, exporter, layer) = setup();

        // The message and the trailer frame in one chunk, the trailer split
        // across the next.
        let mut chunk = message(b"tip").to_vec();
        let trailer = web_trailer(5);
        chunk.extend_from_slice(&trailer[..3]);

        let service = layer.layer(stub(
            vec![("content-type", "application/grpc-web+proto")],
            vec![Frame::data(chunk.into()), Frame::data(trailer.slice(3..))],
        ));

        let mut body = service
            .oneshot(request(UNARY, "application/grpc-web+proto"))
            .await
            .unwrap()
            .into_body();
        read_to_end(&mut body).await;

        assert_eq!(
            collect(&provider, &exporter),
            [
                point("dolos.rpc.server.active_requests", "ReadTip", None, "0"),
                point(
                    "rpc.server.call.duration",
                    "ReadTip",
                    Some("NOT_FOUND"),
                    "count=1"
                ),
            ]
        );
    }

    #[tokio::test]
    async fn a_grpc_web_stream_reports_the_status_of_its_trailer_frame() {
        let (provider, exporter, layer) = setup();
        let service = layer.layer(stub(
            vec![("content-type", "application/grpc-web+proto")],
            vec![
                Frame::data(message(b"one")),
                Frame::data(message(b"two")),
                Frame::data(web_trailer(14)),
            ],
        ));

        let mut body = service
            .oneshot(request(STREAM, "application/grpc-web+proto"))
            .await
            .unwrap()
            .into_body();
        read_to_end(&mut body).await;

        assert_eq!(
            collect(&provider, &exporter),
            [
                point("dolos.rpc.server.active_requests", "FollowTip", None, "0"),
                point("dolos.rpc.server.active_streams", "FollowTip", None, "0"),
                point(
                    "dolos.rpc.server.streams.ended",
                    "FollowTip",
                    Some("UNAVAILABLE"),
                    "1"
                ),
                point(
                    "rpc.server.call.duration",
                    "FollowTip",
                    Some("OK"),
                    "count=1"
                ),
            ]
        );
    }

    #[tokio::test]
    async fn a_grpc_web_text_call_reports_the_status_of_its_encoded_trailer_frame() {
        let (provider, exporter, layer) = setup();
        let encode = |bytes: Bytes| -> Bytes {
            base64::engine::general_purpose::STANDARD
                .encode(bytes)
                .into()
        };
        let service = layer.layer(stub(
            vec![("content-type", "application/grpc-web-text+proto")],
            vec![
                Frame::data(encode(message(b"tip"))),
                Frame::data(encode(web_trailer(3))),
            ],
        ));

        let mut body = service
            .oneshot(request(UNARY, "application/grpc-web-text"))
            .await
            .unwrap()
            .into_body();
        read_to_end(&mut body).await;

        assert_eq!(
            collect(&provider, &exporter),
            [
                point("dolos.rpc.server.active_requests", "ReadTip", None, "0"),
                point(
                    "rpc.server.call.duration",
                    "ReadTip",
                    Some("INVALID_ARGUMENT"),
                    "count=1"
                ),
            ]
        );
    }

    #[tokio::test]
    async fn an_unknown_path_is_labelled_unknown() {
        let (provider, exporter, layer) = setup();
        let service = layer.layer(stub(
            vec![("content-type", "application/grpc"), ("grpc-status", "12")],
            vec![],
        ));

        drop(
            service
                .oneshot(request("/no.Such/Method", "application/grpc"))
                .await
                .unwrap(),
        );

        assert_eq!(
            collect(&provider, &exporter),
            [
                "dolos.rpc.server.active_requests{rpc.method=unknown,rpc.service=unknown,surface=grpc} 0",
                "rpc.server.call.duration{rpc.method=unknown,rpc.response.status_code=UNIMPLEMENTED,rpc.service=unknown,surface=grpc} count=1",
            ]
        );
    }

    #[tokio::test]
    async fn a_preflight_request_records_nothing() {
        let (provider, exporter, layer) = setup();
        let service = layer.layer(stub(vec![], vec![]));

        let preflight = Request::builder()
            .method(HttpMethod::OPTIONS)
            .uri(UNARY)
            .header("origin", "https://example.com")
            .header("access-control-request-method", "POST")
            .header("access-control-request-headers", "content-type,x-grpc-web")
            .body(())
            .unwrap();

        let mut body = service.oneshot(preflight).await.unwrap().into_body();
        read_to_end(&mut body).await;

        assert!(collect(&provider, &exporter).is_empty());
    }

    #[test]
    fn status_values_map_to_their_names() {
        assert_eq!(status_name(b"0"), "OK");
        assert_eq!(status_name(b"16"), "UNAUTHENTICATED");
        assert_eq!(status_name(b"17"), "UNKNOWN");
        assert_eq!(status_name(b"x"), "UNKNOWN");
        assert_eq!(
            block_status(b"grpc-message:x\r\nGrpc-Status: 4\r\n"),
            Some("DEADLINE_EXCEEDED")
        );
        assert_eq!(block_status(b"grpc-message:x\r\n"), None);
    }
}
