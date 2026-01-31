//! ground-truth subcommands.

use anyhow::{Context, Result};
use clap::Subcommand;
use postgres::{Client, NoTls};

pub mod compare;
mod delegation;
mod epochs;
mod eras;
pub mod generate;
mod query;
mod rewards;
mod stake;

/// Connect to DBSync using the provided URL.
fn connect_to_dbsync(dbsync_url: &str) -> Result<Client> {
    let client = Client::connect(dbsync_url, NoTls)
        .with_context(|| format!("Failed to connect to DBSync: {}", dbsync_url))?;
    Ok(client)
}

#[derive(Debug, Subcommand)]
pub enum GroundTruthCmd {
    /// Generate ground-truth fixtures from cardano-db-sync
    Generate(generate::GenerateArgs),

    /// Compare DBSync ground-truth CSVs with Dolos CSV output
    Compare(compare::CompareArgs),

    /// Query DBSync for a specific entity and epoch
    Query(query::QueryArgs),
}

pub fn run(cmd: GroundTruthCmd) -> Result<()> {
    match cmd {
        GroundTruthCmd::Generate(args) => generate::run(&args),
        GroundTruthCmd::Compare(args) => compare::run(&args),
        GroundTruthCmd::Query(args) => query::run(&args),
    }
}
