//! Rewards dataset: fetch from DBSync, write to CSV, dump from Dolos.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone)]
pub(super) struct RewardRow {
    pub stake: String,
    pub pool: String,
    pub amount: String,
    pub reward_type: String,
    pub earned_epoch: String,
}

pub(super) fn fetch(dbsync_url: &str, epoch: u64) -> Result<Vec<RewardRow>> {
    let mut client = super::connect_to_dbsync(dbsync_url)?;
    let epoch = i64::try_from(epoch)
        .map_err(|_| anyhow::anyhow!("epoch out of range for dbsync (expected i64)"))?;

    let query = r#"
        SELECT
            sa.view AS stake,
            COALESCE(ph.view, '') AS pool,
            r.amount::text AS amount,
            r.type::text AS reward_type,
            r.earned_epoch::text AS earned_epoch
        FROM reward r
        JOIN stake_address sa ON sa.id = r.addr_id
        LEFT JOIN pool_hash ph ON ph.id = r.pool_id
        WHERE r.earned_epoch = $1
          AND r.type IN ('leader', 'member')
          AND r.amount > 0
        ORDER BY sa.view, COALESCE(ph.view, ''), r.type::text
    "#;

    let rows = client
        .query(query, &[&epoch])
        .with_context(|| "failed to query rewards")?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(RewardRow {
            stake: row.get(0),
            pool: row.get(1),
            amount: row.get(2),
            reward_type: row.get(3),
            earned_epoch: row.get(4),
        });
    }

    Ok(out)
}

pub(super) fn write_csv(path: &Path, rows: &[RewardRow]) -> Result<()> {
    let mut file =
        File::create(path).with_context(|| format!("writing rewards csv: {}", path.display()))?;
    writeln!(file, "stake,pool,amount,type,earned_epoch")?;
    for row in rows {
        writeln!(
            file,
            "{},{},{},{},{}",
            row.stake, row.pool, row.amount, row.reward_type, row.earned_epoch
        )?;
    }
    Ok(())
}
