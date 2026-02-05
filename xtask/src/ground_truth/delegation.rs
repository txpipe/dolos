//! Pool delegation dataset: fetch from DBSync and write to CSV.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone)]
pub(super) struct PoolDelegationRow {
    pub pool_bech32: String,
    pub pool_hash: String,
    pub total_lovelace: String,
}

pub(super) fn fetch(dbsync_url: &str, epoch: u64) -> Result<Vec<PoolDelegationRow>> {
    let mut client = super::connect_to_dbsync(dbsync_url)?;
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

pub(super) fn write_csv(path: &Path, rows: &[PoolDelegationRow]) -> Result<()> {
    let mut file = File::create(path)
        .with_context(|| format!("writing pools csv: {}", path.display()))?;
    writeln!(file, "pool_bech32,pool_hash,total_lovelace")?;
    for row in rows {
        writeln!(
            file,
            "{},{},{}",
            row.pool_bech32, row.pool_hash, row.total_lovelace
        )?;
    }
    Ok(())
}
