//! ground-truth query subcommand: query DBSync entities directly.

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};

use crate::config::{load_xtask_config, Network};

#[derive(Debug, Clone, ValueEnum)]
pub enum QueryEntity {
    Pools,
    Accounts,
    Rewards,
}

#[derive(Debug, Args)]
pub struct QueryArgs {
    /// Entity to query
    #[arg(value_enum)]
    pub entity: QueryEntity,

    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Epoch number to query
    #[arg(long)]
    pub epoch: u64,
}

pub fn run(args: &QueryArgs) -> Result<()> {
    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;

    let dbsync_url = xtask_config
        .dbsync
        .url_for_network(&args.network)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no dbsync URL configured for network '{}' in xtask.toml",
                args.network.as_str()
            )
        })?;

    match args.entity {
        QueryEntity::Pools => {
            let rows = super::delegation::fetch(dbsync_url, args.epoch)?;
            println!("pool_bech32,pool_hash,total_lovelace");
            for row in rows {
                println!(
                    "{},{},{}",
                    row.pool_bech32, row.pool_hash, row.total_lovelace
                );
            }
        }
        QueryEntity::Accounts => {
            let rows = super::stake::fetch(dbsync_url, args.epoch)?;
            println!("stake,pool,lovelace");
            for row in rows {
                println!("{},{},{}", row.stake, row.pool, row.lovelace);
            }
        }
        QueryEntity::Rewards => {
            let rows = super::rewards::fetch(dbsync_url, args.epoch)?;
            println!("stake,pool,amount,type,earned_epoch");
            for row in rows {
                println!(
                    "{},{},{},{},{}",
                    row.stake, row.pool, row.amount, row.reward_type, row.earned_epoch
                );
            }
        }
    }

    Ok(())
}
