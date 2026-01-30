//! Compare RUPD snapshot CSVs with DBSync data.

use anyhow::{Context, Result};
use clap::Args;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::config::{load_xtask_config, Network};
use crate::dbsync_query::{
    fetch_account_stake, fetch_pool_delegation, AccountStakeRow, PoolDelegationRow,
};

/// Arguments for comparing RUPD snapshot dumps with DBSync output.
#[derive(Debug, Args)]
pub struct CompareArgs {
    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Snapshot epoch to compare
    #[arg(long)]
    pub epoch: u64,

    /// Instance epoch (used to resolve instance path)
    #[arg(long)]
    pub instance_epoch: u64,

    /// Optional instance name override
    #[arg(long)]
    pub instance_name: Option<String>,

    /// Number of sample rows to show per category
    #[arg(long, default_value_t = 20)]
    pub sample: usize,
}

pub fn run(args: &CompareArgs) -> Result<()> {
    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;

    let instances_root = crate::util::resolve_path(&repo_root, &xtask_config.instances_root);
    let instance_name = args
        .instance_name
        .clone()
        .unwrap_or_else(|| format!("test-{}-{}", args.network.as_str(), args.instance_epoch));
    let instance_dir = instances_root.join(&instance_name);
    let storage_path = resolve_storage_path(&instance_dir)?;

    let csv_epoch = args.epoch.saturating_sub(2);
    let rupd_dir = storage_path.join("rupd-snapshot");
    let pools_path = rupd_dir.join(format!("{}-pools.csv", csv_epoch));
    let accounts_path = rupd_dir.join(format!("{}-accounts.csv", csv_epoch));

    let rupd_pools = load_pools_csv(&pools_path)?;
    let rupd_accounts = load_accounts_csv(&accounts_path)?;

    let dbsync_pools = fetch_pool_delegation(&args.network, args.epoch)?;
    let dbsync_accounts = fetch_account_stake(&args.network, args.epoch)?;

    println!(
        "Comparing pools (dbsync epoch {}, csv epoch {})",
        args.epoch, csv_epoch
    );
    compare_pools(rupd_pools, dbsync_pools, args.sample);

    println!(
        "\nComparing accounts (dbsync epoch {}, csv epoch {})",
        args.epoch, csv_epoch
    );
    compare_accounts(rupd_accounts, dbsync_accounts, args.sample);

    Ok(())
}

fn resolve_storage_path(instance_dir: &Path) -> Result<PathBuf> {
    let config_path = instance_dir.join("dolos.toml");
    if config_path.exists() {
        let raw = std::fs::read_to_string(&config_path)
            .with_context(|| format!("reading config: {}", config_path.display()))?;
        let config: dolos_core::config::RootConfig = toml::from_str(&raw)
            .with_context(|| format!("parsing config: {}", config_path.display()))?;
        return Ok(config.storage.path);
    }

    Ok(instance_dir.join("data"))
}

fn load_pools_csv(path: &Path) -> Result<Vec<PoolDelegationRow>> {
    let rows = read_csv(path)?;
    let expected = ["pool_bech32", "pool_hash", "total_lovelace"];
    validate_header(&rows.header, &expected, path)?;

    let mut out = Vec::with_capacity(rows.data.len());
    for row in rows.data {
        ensure_cols(&row, 3, path)?;
        out.push(PoolDelegationRow {
            pool_bech32: row[0].clone(),
            pool_hash: row[1].clone(),
            total_lovelace: row[2].clone(),
        });
    }

    Ok(out)
}

fn load_accounts_csv(path: &Path) -> Result<Vec<AccountStakeRow>> {
    let rows = read_csv(path)?;
    let expected = ["stake", "pool", "lovelace"];
    validate_header(&rows.header, &expected, path)?;

    let mut out = Vec::with_capacity(rows.data.len());
    for row in rows.data {
        ensure_cols(&row, 3, path)?;
        out.push(AccountStakeRow {
            stake: row[0].clone(),
            pool: row[1].clone(),
            lovelace: row[2].clone(),
        });
    }

    Ok(out)
}

fn compare_pools(rupd: Vec<PoolDelegationRow>, dbsync: Vec<PoolDelegationRow>, sample: usize) {
    let mut rupd_map: HashMap<String, PoolDelegationRow> = HashMap::new();
    for row in rupd {
        rupd_map.insert(row.pool_bech32.clone(), row);
    }

    let mut db_map: HashMap<String, PoolDelegationRow> = HashMap::new();
    for row in dbsync {
        db_map.insert(row.pool_bech32.clone(), row);
    }

    let mut missing_in_rupd = Vec::new();
    let mut missing_in_dbsync = Vec::new();
    let mut mismatched = Vec::new();

    for (key, db_row) in &db_map {
        match rupd_map.get(key) {
            None => missing_in_rupd.push(key.clone()),
            Some(rupd_row) => {
                if rupd_row.pool_hash != db_row.pool_hash
                    || rupd_row.total_lovelace != db_row.total_lovelace
                {
                    mismatched.push((key.clone(), rupd_row.clone(), db_row.clone()));
                }
            }
        }
    }

    for key in rupd_map.keys() {
        if !db_map.contains_key(key) {
            missing_in_dbsync.push(key.clone());
        }
    }

    missing_in_rupd.sort();
    missing_in_dbsync.sort();
    mismatched.sort_by(|a, b| a.0.cmp(&b.0));

    let total_common = db_map.len().saturating_sub(missing_in_rupd.len());
    let matches = total_common.saturating_sub(mismatched.len());

    println!("- missing in RUPD: {}", missing_in_rupd.len());
    for key in missing_in_rupd.iter().take(sample) {
        println!("  - {}", key);
    }

    println!("- missing in DBSync: {}", missing_in_dbsync.len());
    for key in missing_in_dbsync.iter().take(sample) {
        println!("  - {}", key);
    }

    println!("- matches: {}", matches);
    println!("- mismatched: {}", mismatched.len());
    for (key, rupd_row, db_row) in mismatched.iter().take(sample) {
        println!(
            "  - {} | rupd: {} {} | dbsync: {} {}",
            key,
            rupd_row.pool_hash,
            rupd_row.total_lovelace,
            db_row.pool_hash,
            db_row.total_lovelace
        );
    }
}

fn compare_accounts(rupd: Vec<AccountStakeRow>, dbsync: Vec<AccountStakeRow>, sample: usize) {
    let mut rupd_map: HashMap<(String, String), AccountStakeRow> = HashMap::new();
    for row in rupd {
        rupd_map.insert((row.stake.clone(), row.pool.clone()), row);
    }

    let mut db_map: HashMap<(String, String), AccountStakeRow> = HashMap::new();
    for row in dbsync {
        db_map.insert((row.stake.clone(), row.pool.clone()), row);
    }

    let mut missing_in_rupd = Vec::new();
    let mut missing_in_dbsync = Vec::new();
    let mut mismatched = Vec::new();

    for (key, db_row) in &db_map {
        match rupd_map.get(key) {
            None => missing_in_rupd.push(key.clone()),
            Some(rupd_row) => {
                if rupd_row.lovelace != db_row.lovelace {
                    mismatched.push((key.clone(), rupd_row.clone(), db_row.clone()));
                }
            }
        }
    }

    for key in rupd_map.keys() {
        if !db_map.contains_key(key) {
            missing_in_dbsync.push(key.clone());
        }
    }

    missing_in_rupd.sort_by(|a, b| (a.0.clone(), a.1.clone()).cmp(&(b.0.clone(), b.1.clone())));
    missing_in_dbsync.sort_by(|a, b| (a.0.clone(), a.1.clone()).cmp(&(b.0.clone(), b.1.clone())));
    mismatched
        .sort_by(|a, b| (a.0 .0.clone(), a.0 .1.clone()).cmp(&(b.0 .0.clone(), b.0 .1.clone())));

    let total_common = db_map.len().saturating_sub(missing_in_rupd.len());
    let matches = total_common.saturating_sub(mismatched.len());

    println!("- missing in RUPD: {}", missing_in_rupd.len());
    for (stake, pool) in missing_in_rupd.iter().take(sample) {
        println!("  - {} {}", stake, pool);
    }

    println!("- missing in DBSync: {}", missing_in_dbsync.len());
    for (stake, pool) in missing_in_dbsync.iter().take(sample) {
        println!("  - {} {}", stake, pool);
    }

    println!("- matches: {}", matches);
    println!("- mismatched: {}", mismatched.len());
    for ((stake, pool), rupd_row, db_row) in mismatched.iter().take(sample) {
        println!(
            "  - {} {} | rupd: {} | dbsync: {}",
            stake, pool, rupd_row.lovelace, db_row.lovelace
        );
    }
}

struct CsvData {
    header: Vec<String>,
    data: Vec<Vec<String>>,
}

fn read_csv(path: &Path) -> Result<CsvData> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading csv: {}", path.display()))?;
    let mut lines = raw.lines();

    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty csv: {}", path.display()))?;
    let header = split_csv_line(header);

    let mut data = Vec::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        data.push(split_csv_line(line));
    }

    Ok(CsvData { header, data })
}

fn split_csv_line(line: &str) -> Vec<String> {
    line.split(',').map(|s| s.trim().to_string()).collect()
}

fn validate_header(header: &[String], expected: &[&str], path: &Path) -> Result<()> {
    let expected_vec: Vec<String> = expected.iter().map(|x| x.to_string()).collect();
    if header != expected_vec {
        anyhow::bail!(
            "unexpected header in {}: got {:?}, expected {:?}",
            path.display(),
            header,
            expected_vec
        );
    }
    Ok(())
}

fn ensure_cols(row: &[String], expected: usize, path: &Path) -> Result<()> {
    if row.len() != expected {
        anyhow::bail!(
            "unexpected column count in {}: got {}, expected {}",
            path.display(),
            row.len(),
            expected
        );
    }
    Ok(())
}
