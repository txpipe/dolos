//! Account stake dataset: fetch from DBSync and write to CSV.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone)]
pub(super) struct AccountStakeRow {
    pub stake: String,
    pub pool: String,
    pub lovelace: String,
}

pub(super) fn fetch(dbsync_url: &str, epoch: u64) -> Result<Vec<AccountStakeRow>> {
    let mut client = super::connect_to_dbsync(dbsync_url)?;
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

pub(super) fn write_csv(path: &Path, rows: &[AccountStakeRow]) -> Result<()> {
    let mut file =
        File::create(path).with_context(|| format!("writing accounts csv: {}", path.display()))?;
    writeln!(file, "stake,pool,lovelace")?;
    for row in rows {
        writeln!(file, "{},{},{}", row.stake, row.pool, row.lovelace)?;
    }
    Ok(())
}
