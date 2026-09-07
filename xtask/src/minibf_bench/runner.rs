//! HTTP benchmarking with concurrent execution.

use anyhow::Result;
use hdrhistogram::Histogram;
use reqwest::{Client, Method, StatusCode};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;

use crate::minibf_bench::args::BenchArgs;

#[derive(Debug, Clone)]
pub struct TestRequest {
    pub endpoint_name: String,
    pub path: String,
    pub method: Method,
}

#[derive(Debug, Clone)]
pub struct RequestResult {
    pub endpoint_name: String,
    pub path: String,
    pub latency_micros: u64,
    pub status: StatusCode,
    pub success: bool,
    pub error: Option<String>,
}

pub struct BenchmarkRunner {
    client: Client,
    args: BenchArgs,
}

impl BenchmarkRunner {
    pub fn new(args: BenchArgs) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(args.concurrency)
            .build()?;

        Ok(Self { client, args })
    }

    pub async fn run_warmup(&self, requests: Vec<TestRequest>) -> Result<()> {
        println!("Running {} warmup requests...", self.args.warmup);

        let semaphore = Arc::new(Semaphore::new(self.args.concurrency));

        for (i, req) in requests.iter().cycle().take(self.args.warmup).enumerate() {
            let permit = semaphore.clone().acquire_owned().await?;
            let client = self.client.clone();
            let request = req.clone();
            let base_url = self.args.url.clone();

            tokio::spawn(async move {
                let url = format!("{}{}", base_url, request.path);
                let _ = client.request(request.method, url).send().await;
                drop(permit);
            });

            if (i + 1) % 100 == 0 {
                println!("  Warmup {}/{}", i + 1, self.args.warmup);
            }
        }

        Ok(())
    }

    pub async fn run_benchmark(
        &self,
        requests: Vec<TestRequest>,
    ) -> Result<(Vec<RequestResult>, Histogram<u64>)> {
        println!("Running {} benchmark requests...", self.args.requests);

        let semaphore = Arc::new(Semaphore::new(self.args.concurrency));
        let (tx, mut rx) = mpsc::channel::<RequestResult>(1000);

        // Start result collector before spawning workers to avoid deadlock
        let collector = tokio::spawn(async move {
            let mut results = Vec::new();
            let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();

            while let Some(result) = rx.recv().await {
                let _ = histogram.record(result.latency_micros);
                results.push(result);
            }

            (results, histogram)
        });

        // Spawn workers
        let total = self.args.requests;
        for (i, req) in requests.iter().cycle().take(total).enumerate() {
            let permit = semaphore.clone().acquire_owned().await?;
            let client = self.client.clone();
            let request = req.clone();
            let base_url = self.args.url.clone();
            let tx = tx.clone();

            tokio::spawn(async move {
                let start = Instant::now();
                let url = format!("{}{}", base_url, request.path);

                let result = match client.request(request.method.clone(), &url).send().await {
                    Ok(resp) => {
                        let status = resp.status();
                        let success = status.is_success();
                        RequestResult {
                            endpoint_name: request.endpoint_name.clone(),
                            path: request.path,
                            latency_micros: start.elapsed().as_micros() as u64,
                            status,
                            success,
                            error: None,
                        }
                    }
                    Err(e) => RequestResult {
                        endpoint_name: request.endpoint_name.clone(),
                        path: request.path,
                        latency_micros: start.elapsed().as_micros() as u64,
                        status: StatusCode::REQUEST_TIMEOUT,
                        success: false,
                        error: Some(e.to_string()),
                    },
                };

                let _ = tx.send(result).await;
                drop(permit);
            });

            if (i + 1) % 1000 == 0 {
                println!("  Queued {}/{} requests", i + 1, total);
            }
        }

        // Drop original sender so collector knows when done
        drop(tx);

        // Wait for collector to finish
        let (results, histogram) = collector.await?;

        Ok((results, histogram))
    }
}
