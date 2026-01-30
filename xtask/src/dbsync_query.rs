//! DBSync query helpers for debugging.

use anyhow::{bail, Context, Result};
use clap::Args;
use postgres::{Client, NoTls};

use crate::config::{load_xtask_config, Network};

/// Arguments for pool delegation query.
#[derive(Debug, Args)]
pub struct PoolDelegationArgs {
    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Epoch number to query
    #[arg(long)]
    pub epoch: u64,
}

/// Arguments for account stake query.
#[derive(Debug, Args)]
pub struct AccountStakeArgs {
    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Epoch number to query
    #[arg(long)]
    pub epoch: u64,
}

#[derive(Debug, Clone)]
pub struct PoolDelegationRow {
    pub pool_bech32: String,
    pub pool_hash: String,
    pub total_lovelace: String,
}

#[derive(Debug, Clone)]
pub struct AccountStakeRow {
    pub stake: String,
    pub pool: String,
    pub lovelace: String,
}

pub fn run_pool_delegation(args: &PoolDelegationArgs) -> Result<()> {
    let rows = fetch_pool_delegation(&args.network, args.epoch)?;

    println!("pool_bech32,pool_hash,total_lovelace");
    for row in rows {
        println!(
            "{},{},{}",
            row.pool_bech32, row.pool_hash, row.total_lovelace
        );
    }

    Ok(())
}

pub fn run_account_stake(args: &AccountStakeArgs) -> Result<()> {
    let rows = fetch_account_stake(&args.network, args.epoch)?;

    println!("stake,pool,lovelace");
    for row in rows {
        println!("{},{},{}", row.stake, row.pool, row.lovelace);
    }

    Ok(())
}

pub fn fetch_pool_delegation(network: &Network, epoch: u64) -> Result<Vec<PoolDelegationRow>> {
    let mut client = connect_to_dbsync(network)?;
    let epoch = i32::try_from(epoch)
        .map_err(|_| anyhow::anyhow!("epoch out of range for dbsync (expected i32)"))?;

    let query = r#"
        SELECT
            ph.view AS pool_bech32,
            encode(ph.hash_raw, 'hex') AS pool_hash,
            SUM(es.amount)::text AS total_lovelace
        FROM epoch_stake es
        JOIN pool_hash ph ON ph.id = es.pool_id
        WHERE es.epoch_no = $1
        GROUP BY ph.view, ph.hash_raw
        ORDER BY ph.view
    "#;

    let rows = client
        .query(query, &[&epoch])
        .with_context(|| "failed to query pool delegation")?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(PoolDelegationRow {
            pool_bech32: row.get(0),
            pool_hash: row.get(1),
            total_lovelace: row.get(2),
        });
    }

    Ok(out)
}

pub fn fetch_account_stake(network: &Network, epoch: u64) -> Result<Vec<AccountStakeRow>> {
    let mut client = connect_to_dbsync(network)?;
    let epoch = i32::try_from(epoch)
        .map_err(|_| anyhow::anyhow!("epoch out of range for dbsync (expected i32)"))?;

    let query = r#"
        SELECT
            sa.view AS stake,
            ph.view AS pool,
            es.amount::text AS lovelace
        FROM epoch_stake es
        JOIN stake_address sa ON sa.id = es.addr_id
        JOIN pool_hash ph ON ph.id = es.pool_id
        WHERE es.epoch_no = $1
        ORDER BY sa.view, ph.view
    "#;

    let rows = client
        .query(query, &[&epoch])
        .with_context(|| "failed to query account stake")?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(AccountStakeRow {
            stake: row.get(0),
            pool: row.get(1),
            lovelace: row.get(2),
        });
    }

    Ok(out)
}

fn connect_to_dbsync(network: &Network) -> Result<Client> {
    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;
    let dbsync_url = xtask_config
        .dbsync
        .url_for_network(network)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no dbsync URL configured for network '{}' in xtask.toml",
                network.as_str()
            )
        })?;

    if dbsync_url.trim().is_empty() {
        bail!("dbsync URL for network '{}' is empty", network.as_str());
    }

    let client = Client::connect(dbsync_url, NoTls)
        .with_context(|| format!("failed to connect to DBSync: {}", dbsync_url))?;
    Ok(client)
}
