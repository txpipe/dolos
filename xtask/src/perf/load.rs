use super::measure;
use anyhow::Context;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

struct Outcome {
    latency: u64,
    service: u64,
    bytes: usize,
    error: Option<String>,
}

fn collect(outcome: Outcome, stats: &mut LoadStats, timeout: Duration) {
    stats.latency.record(outcome.latency.max(1)).unwrap();
    stats.service.record(outcome.service.max(1)).unwrap();
    stats.response_bytes += outcome.bytes;
    if let Some(error) = outcome.error {
        stats.errors += 1;
        if stats.first_error.is_none() {
            stats.first_error = Some(error);
        }
    } else if outcome.latency > timeout.as_nanos() as u64 {
        stats.timeouts += 1;
    } else {
        stats.completed += 1;
    }
}

struct LoadStats {
    latency: hdrhistogram::Histogram<u64>,
    service: hdrhistogram::Histogram<u64>,
    completed: usize,
    errors: usize,
    timeouts: usize,
    rejected: usize,
    response_bytes: usize,
    peak_in_flight: usize,
    first_error: Option<String>,
}

impl Default for LoadStats {
    fn default() -> Self {
        Self {
            latency: measure::histogram(),
            service: measure::histogram(),
            completed: 0,
            errors: 0,
            timeouts: 0,
            rejected: 0,
            response_bytes: 0,
            peak_in_flight: 0,
            first_error: None,
        }
    }
}

pub async fn drive_requests<Request, Response>(
    request: Request,
    requests: usize,
    concurrency: usize,
    rate: u64,
    timeout: Duration,
) -> anyhow::Result<Value>
where
    Request: Fn() -> Response,
    Response: std::future::Future<Output = anyhow::Result<usize>> + Send + 'static,
{
    anyhow::ensure!(
        requests > 0 && concurrency > 0,
        "requests and concurrency must be positive"
    );
    let mut tasks = JoinSet::new();
    let mut stats = LoadStats::default();
    let start = Instant::now();
    for index in 0..requests {
        let scheduled = if rate == 0 {
            Instant::now()
        } else {
            start + Duration::from_secs_f64(index as f64 / rate as f64)
        };
        if rate > 0 {
            tokio::time::sleep_until(scheduled.into()).await;
        }
        while let Some(outcome) = tasks.try_join_next() {
            collect(outcome?, &mut stats, timeout);
        }
        if tasks.len() >= concurrency {
            if rate > 0 {
                stats.rejected += 1;
                continue;
            }
            collect(
                tasks.join_next().await.context("missing request")??,
                &mut stats,
                timeout,
            );
        }
        let scheduled = if rate == 0 { Instant::now() } else { scheduled };
        let response = request();
        tasks.spawn(async move {
            let service_start = Instant::now();
            let result = response.await;
            Outcome {
                latency: scheduled.elapsed().as_nanos().min(3_600_000_000_000) as u64,
                service: service_start.elapsed().as_nanos().min(3_600_000_000_000) as u64,
                bytes: result.as_ref().copied().unwrap_or(0),
                error: result.err().map(|error| error.to_string()),
            }
        });
        stats.peak_in_flight = stats.peak_in_flight.max(tasks.len());
    }
    while let Some(outcome) = tasks.join_next().await {
        collect(outcome?, &mut stats, timeout);
    }
    if rate > 0 {
        tokio::time::sleep_until(
            (start + Duration::from_secs_f64(requests as f64 / rate as f64)).into(),
        )
        .await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    Ok(json!({
        "requests": requests, "completed": stats.completed, "errors": stats.errors,
        "timeouts": stats.timeouts, "rejected": stats.rejected,
        "first_error": stats.first_error, "elapsed_seconds": elapsed,
        "completed_per_second": stats.completed as f64 / elapsed,
        "latency": measure::histogram_json(&stats.latency),
        "service_latency": measure::histogram_json(&stats.service),
        "response_bytes": stats.response_bytes, "peak_in_flight": stats.peak_in_flight,
    }))
}
