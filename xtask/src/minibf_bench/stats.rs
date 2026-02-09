//! Statistical analysis using HDR histogram.

use anyhow::Result;
use hdrhistogram::Histogram;

use crate::minibf_bench::runner::RequestResult;

/// Statistics for a single endpoint
#[derive(Debug, Clone)]
pub struct EndpointStats {
    pub name: String,
    pub path: String,
    pub requests: u64,
    pub success_count: u64,
    pub failure_count: u64,
    pub latency_p50_micros: u64,
    pub latency_p95_micros: u64,
    pub latency_p99_micros: u64,
    pub latency_min_micros: u64,
    pub latency_max_micros: u64,
}

impl EndpointStats {
    pub fn success_rate(&self) -> f64 {
        if self.requests == 0 {
            0.0
        } else {
            self.success_count as f64 / self.requests as f64
        }
    }
}

pub fn calculate_endpoint_stats(results: &[RequestResult]) -> Result<Vec<EndpointStats>> {
    use std::collections::HashMap;

    let mut endpoint_data: HashMap<String, Vec<&RequestResult>> = HashMap::new();

    for result in results {
        endpoint_data
            .entry(result.endpoint_name.clone())
            .or_default()
            .push(result);
    }

    let mut stats = Vec::new();

    for (name, results) in endpoint_data {
        let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
        let mut success_count = 0u64;
        let mut failure_count = 0u64;

        for result in &results {
            histogram.record(result.latency_micros)?;
            if result.success {
                success_count += 1;
            } else {
                failure_count += 1;
            }
        }

        stats.push(EndpointStats {
            path: results.first().map(|r| r.path.clone()).unwrap_or_default(),
            name,
            requests: results.len() as u64,
            success_count,
            failure_count,
            latency_p50_micros: histogram.value_at_quantile(0.50),
            latency_p95_micros: histogram.value_at_quantile(0.95),
            latency_p99_micros: histogram.value_at_quantile(0.99),
            latency_min_micros: histogram.min(),
            latency_max_micros: histogram.max(),
        });
    }

    // Sort by name for consistent output
    stats.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(stats)
}
