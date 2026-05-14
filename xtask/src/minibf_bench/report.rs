//! JSON report generation for CI ingestion.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::minibf_bench::{args::BenchArgs, stats::EndpointStats, vectors::TestVectors};

#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub metadata: ReportMetadata,
    pub summary: ReportSummary,
    pub endpoints: Vec<EndpointReport>,
    pub pagination_analysis: PaginationAnalysis,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReportMetadata {
    pub timestamp: String,
    pub url: String,
    pub network: String,
    pub total_requests: usize,
    pub concurrency: usize,
    pub warmup_requests: usize,
    pub duration_secs: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReportSummary {
    pub total_requests: usize,
    pub successful_requests: usize,
    pub failed_requests: usize,
    pub requests_per_second: f64,
    pub overall_p50_ms: f64,
    pub overall_p95_ms: f64,
    pub overall_p99_ms: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EndpointReport {
    pub name: String,
    pub path: String,
    pub requests: u64,
    pub success_rate: f64,
    pub latency_ms: LatencyStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LatencyStats {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PaginationAnalysis {
    pub scan_limit: u64,
    pub page_size_tested: u64,
    pub tests: Vec<PaginationTestResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PaginationTestResult {
    pub page: u64,
    pub count: u64,
    pub p50_ms: Option<f64>,
    pub success_rate: f64,
    pub error: Option<String>,
}

pub fn generate_report(
    args: &BenchArgs,
    duration: std::time::Duration,
    endpoint_stats: Vec<EndpointStats>,
    vectors: &TestVectors,
) -> Result<BenchmarkReport> {
    let total_requests: usize = endpoint_stats.iter().map(|s| s.requests as usize).sum();
    let successful_requests: usize = endpoint_stats
        .iter()
        .map(|s| s.success_count as usize)
        .sum();
    let failed_requests = total_requests - successful_requests;
    let rps = total_requests as f64 / duration.as_secs_f64();

    // Calculate overall latencies from all endpoints
    let all_p50: Vec<u64> = endpoint_stats
        .iter()
        .map(|s| s.latency_p50_micros)
        .collect();
    let all_p95: Vec<u64> = endpoint_stats
        .iter()
        .map(|s| s.latency_p95_micros)
        .collect();
    let all_p99: Vec<u64> = endpoint_stats
        .iter()
        .map(|s| s.latency_p99_micros)
        .collect();

    let overall_p50 = all_p50.iter().sum::<u64>() as f64 / all_p50.len().max(1) as f64 / 1000.0;
    let overall_p95 = all_p95.iter().sum::<u64>() as f64 / all_p95.len().max(1) as f64 / 1000.0;
    let overall_p99 = all_p99.iter().sum::<u64>() as f64 / all_p99.len().max(1) as f64 / 1000.0;

    let endpoints: Vec<EndpointReport> = endpoint_stats
        .into_iter()
        .map(|stat| {
            let success_rate = stat.success_rate();
            EndpointReport {
                name: stat.name,
                path: stat.path,
                requests: stat.requests,
                success_rate,
                latency_ms: LatencyStats {
                    p50: stat.latency_p50_micros as f64 / 1000.0,
                    p95: stat.latency_p95_micros as f64 / 1000.0,
                    p99: stat.latency_p99_micros as f64 / 1000.0,
                    min: stat.latency_min_micros as f64 / 1000.0,
                    max: stat.latency_max_micros as f64 / 1000.0,
                },
            }
        })
        .collect();

    // Generate pagination analysis
    let pagination_analysis = generate_pagination_analysis(&endpoints);

    Ok(BenchmarkReport {
        metadata: ReportMetadata {
            timestamp: chrono::Utc::now().to_rfc3339(),
            url: args.url.clone(),
            network: vectors.network.clone(),
            total_requests: args.requests,
            concurrency: args.concurrency,
            warmup_requests: args.warmup,
            duration_secs: duration.as_secs_f64(),
        },
        summary: ReportSummary {
            total_requests,
            successful_requests,
            failed_requests,
            requests_per_second: rps,
            overall_p50_ms: overall_p50,
            overall_p95_ms: overall_p95,
            overall_p99_ms: overall_p99,
        },
        endpoints,
        pagination_analysis,
    })
}

fn generate_pagination_analysis(endpoints: &[EndpointReport]) -> PaginationAnalysis {
    // Find pagination endpoints and analyze their performance
    let mut tests = Vec::new();

    // Check if we have scan limit boundary tests
    for endpoint in endpoints {
        if endpoint.path.contains("page=") {
            // Extract page number from path
            let page = extract_page(&endpoint.path);
            let count = extract_count(&endpoint.path);

            tests.push(PaginationTestResult {
                page,
                count,
                p50_ms: Some(endpoint.latency_ms.p50),
                success_rate: endpoint.success_rate,
                error: if endpoint.success_rate < 1.0 {
                    Some("scan_limit_exceeded".to_string())
                } else {
                    None
                },
            });
        }
    }

    PaginationAnalysis {
        scan_limit: 1000,
        page_size_tested: 100,
        tests,
    }
}

fn extract_page(path: &str) -> u64 {
    path.split("page=")
        .nth(1)
        .and_then(|s| s.split('&').next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

fn extract_count(path: &str) -> u64 {
    path.split("count=")
        .nth(1)
        .and_then(|s| s.split('&').next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
}

pub fn write_report(report: &BenchmarkReport, output_path: Option<&std::path::Path>) -> Result<()> {
    let json = serde_json::to_string_pretty(report)?;

    match output_path {
        Some(path) => {
            std::fs::write(path, json)?;
            println!("Report written to {}", path.display());
        }
        None => {
            println!("{}", json);
        }
    }

    Ok(())
}
