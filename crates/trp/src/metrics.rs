use opentelemetry::{global, metrics::Counter, KeyValue};

#[derive(Clone)]
pub struct Metrics {
    pub requests: Counter<u64>,
}
impl Metrics {
    pub fn new() -> Self {
        let meter = global::meter("dolos-trp");
        let requests = meter.u64_counter("requests").build();
        Self { requests }
    }

    pub fn register_request(&self, method: &str, code: i32) {
        self.requests.add(
            1,
            &[
                KeyValue::new("method", method.to_string()),
                KeyValue::new("code", code.to_string()),
            ],
        );
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
