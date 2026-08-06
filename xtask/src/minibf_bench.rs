//! Minibf endpoint benchmarking tool.
//!
//! Measures p50/p95/p99 latency for Dolos minibf endpoints with
//! pagination boundary detection and chain history spread.

pub mod args;

mod report;
mod runner;
mod sampler;
mod stats;
mod vectors;

use std::time::Instant;

use anyhow::{Context, Result};
use reqwest::Method;
use xshell::Shell;

use crate::config::{load_xtask_config, Network};
use args::BenchArgs;
use runner::{BenchmarkRunner, TestRequest};
use vectors::{load_vectors, TestVectors};

pub fn run(_sh: &Shell, args: &BenchArgs) -> Result<()> {
    let start_time = Instant::now();
    let repo_root = std::env::current_dir().context("detecting repo root")?;

    // Load config
    let config = load_xtask_config(&repo_root)?;
    let dbsync_url = config
        .dbsync
        .url_for_network(&args.network)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No DBSync URL configured for {} in xtask.toml",
                args.network.as_str()
            )
        })?;

    // Load or generate test vectors
    let vectors = load_vectors(&args.network, dbsync_url, args.generate_vectors)?;
    println!("Loaded test vectors for {}", vectors.network);

    // Build chain samples from reference blocks in test vectors
    println!("Sampling chain history from reference blocks...");
    let rt = tokio::runtime::Runtime::new()?;
    let samples = rt.block_on(async {
        let client = reqwest::Client::new();
        sampler::HistoricalSamples::from_vectors(&client, &args.url, &vectors.reference_blocks)
            .await
    })?;

    // Generate test requests
    let requests = generate_requests(args, &vectors, &samples);
    println!(
        "Generated {} test requests across {} endpoints",
        requests.len(),
        requests
            .iter()
            .map(|r| &r.endpoint_name)
            .collect::<std::collections::HashSet<_>>()
            .len()
    );

    // Run benchmark
    let runner = BenchmarkRunner::new(args.clone())?;

    // Warmup
    rt.block_on(runner.run_warmup(requests.clone()))?;

    // Benchmark
    let (results, _histogram) = rt.block_on(runner.run_benchmark(requests))?;

    // Calculate stats
    let endpoint_stats = stats::calculate_endpoint_stats(&results)?;

    // Generate report
    let duration = start_time.elapsed();
    let report = report::generate_report(args, duration, endpoint_stats, &vectors)?;

    // Output
    report::write_report(&report, args.output_file.as_deref())?;

    Ok(())
}

fn generate_requests(
    args: &BenchArgs,
    vectors: &TestVectors,
    samples: &crate::minibf_bench::sampler::HistoricalSamples,
) -> Vec<TestRequest> {
    let mut requests = Vec::new();

    // Accounts endpoints (priority)
    if let Some(stake) = vectors.stake_addresses.first() {
        // /accounts/{stake}
        requests.push(TestRequest {
            endpoint_name: "accounts_by_stake".to_string(),
            path: format!("/accounts/{}", stake.stake_address),
            method: Method::GET,
        });

        if !args.skip_paginated {
            // /accounts/{stake}/addresses with pagination
            for page in [1u64, 5, args.max_page] {
                requests.push(TestRequest {
                    endpoint_name: "accounts_addresses_paginated".to_string(),
                    path: format!(
                        "/accounts/{}/addresses?page={}&count=20",
                        stake.stake_address, page
                    ),
                    method: Method::GET,
                });
            }

            // /accounts/{stake}/utxos with pagination
            for page in [1u64, 5] {
                requests.push(TestRequest {
                    endpoint_name: "accounts_utxos_paginated".to_string(),
                    path: format!(
                        "/accounts/{}/utxos?page={}&count=20",
                        stake.stake_address, page
                    ),
                    method: Method::GET,
                });
            }

            // /accounts/{stake}/delegations
            requests.push(TestRequest {
                endpoint_name: "accounts_delegations".to_string(),
                path: format!(
                    "/accounts/{}/delegations?page=1&count=20",
                    stake.stake_address
                ),
                method: Method::GET,
            });
        }
    }

    // Address endpoints
    for addr in vectors.addresses.iter().take(3) {
        // /addresses/{address}/utxos
        requests.push(TestRequest {
            endpoint_name: "addresses_utxos".to_string(),
            path: format!("/addresses/{}/utxos", addr.address),
            method: Method::GET,
        });

        if !args.skip_paginated {
            // /addresses/{address}/transactions with pagination (scan limit testing)
            for page in [1u64, 5, 10, args.max_page] {
                requests.push(TestRequest {
                    endpoint_name: "addresses_transactions_paginated".to_string(),
                    path: format!(
                        "/addresses/{}/transactions?page={}&count=100",
                        addr.address, page
                    ),
                    method: Method::GET,
                });
            }
        }
    }

    // Block endpoints with history spread
    for (label, sample) in [
        ("early", &samples.early),
        ("mid", &samples.mid),
        ("recent", &samples.recent),
    ] {
        // /blocks/{hash}
        requests.push(TestRequest {
            endpoint_name: format!("blocks_by_hash_{}", label),
            path: format!("/blocks/{}", sample.block_hash),
            method: Method::GET,
        });

        // /blocks/{hash}/txs
        requests.push(TestRequest {
            endpoint_name: format!("blocks_txs_{}", label),
            path: format!("/blocks/{}/txs", sample.block_hash),
            method: Method::GET,
        });

        // Test some transactions from this block
        for (i, tx) in sample.txs.iter().take(3).enumerate() {
            requests.push(TestRequest {
                endpoint_name: format!("txs_by_hash_{}_{}", label, i),
                path: format!("/txs/{}", tx),
                method: Method::GET,
            });
        }
    }

    // Latest block
    requests.push(TestRequest {
        endpoint_name: "blocks_latest".to_string(),
        path: "/blocks/latest".to_string(),
        method: Method::GET,
    });

    // Epoch endpoints
    requests.push(TestRequest {
        endpoint_name: "epochs_latest_parameters".to_string(),
        path: "/epochs/latest/parameters".to_string(),
        method: Method::GET,
    });

    // Network endpoint
    requests.push(TestRequest {
        endpoint_name: "network".to_string(),
        path: "/network".to_string(),
        method: Method::GET,
    });

    requests
}
