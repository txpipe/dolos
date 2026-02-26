use std::str::FromStr;

use dolos_core::sync::SyncExt as _;
use miette::IntoDiagnostic as _;
use pallas::crypto::hash::Hash;

use dolos_core::{config::RootConfig, ChainPoint};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// the slot to rollback to
    #[arg(long)]
    slot: u64,

    /// the hash of the block to rollback to
    #[arg(long)]
    hash: String,
}

#[tokio::main]
pub async fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    let domain = crate::common::setup_domain(config).await?;

    let hash: Hash<32> = Hash::from_str(&args.hash).into_diagnostic()?;
    let point = ChainPoint::Specific(args.slot, hash);

    domain
        .rollback(&point)
        .await
        .map_err(|x| miette::miette!(x.to_string()))?;

    Ok(())
}
