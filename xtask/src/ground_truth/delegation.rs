//! Pool delegation dataset: fetch from DBSync and write to CSV.

use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone)]
pub(super) struct PoolDelegationRow {
    pub pool_bech32: String,
    pub pool_hash: String,
    pub total_lovelace: String,
}

const PAGE_SIZE: i64 = 2_000;

pub(super) fn fetch(dbsync_url: &str, epoch: u64) -> Result<Vec<PoolDelegationRow>> {
    let mut client = super::connect_to_dbsync(dbsync_url)?;
    let epoch = i32::try_from(epoch)
        .map_err(|_| anyhow::anyhow!("epoch out of range for dbsync (expected i32)"))?;

    // Aggregate client-side in pages to avoid server-side timeout on large epochs.
    let query = r#"
        SELECT
            ph.view AS pool_bech32,
            encode(ph.hash_raw, 'hex') AS pool_hash,
            es.amount::bigint
        FROM epoch_stake es
        JOIN pool_hash ph ON ph.id = es.pool_id
        WHERE es.epoch_no = $1
        ORDER BY es.id
        LIMIT $2 OFFSET $3
    "#;

    // pool_bech32 -> (pool_hash, total_lovelace)
    let mut pools: BTreeMap<String, (String, u64)> = BTreeMap::new();
    let mut offset: i64 = 0;

    loop {
        eprintln!("  delegation page offset={offset} limit={PAGE_SIZE}");
        let rows = client
            .query(query, &[&epoch, &PAGE_SIZE, &offset])
            .with_context(|| format!("failed to query pool delegation (offset {})", offset))?;

        let count = rows.len();

        for row in rows {
            let pool_bech32: String = row.get(0);
            let pool_hash: String = row.get(1);
            let amount: i64 = row.get(2);

            let entry = pools
                .entry(pool_bech32)
                .or_insert_with(|| (pool_hash, 0));
            entry.1 += amount as u64;
        }

        if (count as i64) < PAGE_SIZE {
            break;
        }

        offset += PAGE_SIZE;
    }

    let out = pools
        .into_iter()
        .map(|(pool_bech32, (pool_hash, total))| PoolDelegationRow {
            pool_bech32,
            pool_hash,
            total_lovelace: total.to_string(),
        })
        .collect();

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
