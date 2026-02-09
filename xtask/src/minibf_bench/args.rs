//! CLI arguments for minibf-bench command.

use clap::{Args, ValueEnum};
use std::path::PathBuf;

use crate::config::Network;

#[derive(Debug, Clone, Args)]
pub struct BenchArgs {
    /// Target minibf URL
    #[arg(long, default_value = "http://localhost:3000")]
    pub url: String,

    /// Target network (determines test vectors)
    #[arg(long, value_enum, default_value = "preprod")]
    pub network: Network,

    /// Generate fresh test vectors from DBSync
    #[arg(long, action)]
    pub generate_vectors: bool,

    /// Total number of requests
    #[arg(long, default_value = "10000")]
    pub requests: usize,

    /// Concurrent connections
    #[arg(long, default_value = "10")]
    pub concurrency: usize,

    /// Warmup requests (not counted in stats)
    #[arg(long, default_value = "1000")]
    pub warmup: usize,

    /// Output format
    #[arg(long, value_enum, default_value = "json")]
    pub output: OutputFormat,

    /// Output file (default: stdout)
    #[arg(long)]
    pub output_file: Option<PathBuf>,

    /// Maximum page to test for pagination endpoints
    #[arg(long, default_value = "11")]
    pub max_page: u64,

    /// Page sizes to test (comma-separated)
    #[arg(long, default_value = "20,50,100")]
    pub page_sizes: String,

    /// Skip paginated endpoints (focus on simple lookups)
    #[arg(long, action)]
    pub skip_paginated: bool,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputFormat {
    Json,
    Table,
}
