//! Protocol parameters dataset: fetch from DBSync and write to CSV.

use anyhow::{Context, Result};
use std::io::Write;

pub(super) struct PParamsRow {
    pub epoch_no: i64,
    pub protocol_major: i32,
    pub protocol_minor: i32,
    pub min_fee_a: i32,
    pub min_fee_b: i32,
    pub key_deposit: String,
    pub pool_deposit: String,
    pub expansion_rate: f64,
    pub treasury_growth_rate: f64,
    pub decentralisation: f64,
    pub desired_pool_number: i32,
    pub min_pool_cost: String,
    pub influence: f64,
}

/// Fetch protocol parameters from DBSync up to (and including) `max_epoch`.
pub(super) fn fetch(dbsync_url: &str, max_epoch: u64) -> Result<Vec<PParamsRow>> {
    let mut client = super::connect_to_dbsync(dbsync_url)?;

    let query = r#"
        SELECT
            ep.epoch_no::bigint,
            ep.protocol_major,
            ep.protocol_minor,
            ep.min_fee_a,
            ep.min_fee_b,
            ep.key_deposit::text,
            ep.pool_deposit::text,
            ep.monetary_expand_rate,
            ep.treasury_growth_rate,
            ep.decentralisation,
            ep.optimal_pool_count,
            ep.min_pool_cost::text,
            ep.influence
        FROM epoch_param ep
        WHERE ep.epoch_no >= 1 AND ep.epoch_no <= $1
        ORDER BY ep.epoch_no
    "#;

    let max_epoch = i32::try_from(max_epoch)
        .map_err(|_| anyhow::anyhow!("max_epoch out of range for dbsync (expected i32)"))?;
    let rows = client
        .query(query, &[&max_epoch])
        .with_context(|| "Failed to query protocol parameters")?;

    let mut pparams = Vec::new();

    for row in rows {
        pparams.push(PParamsRow {
            epoch_no: row.get(0),
            protocol_major: row.get(1),
            protocol_minor: row.get(2),
            min_fee_a: row.get(3),
            min_fee_b: row.get(4),
            key_deposit: row.get(5),
            pool_deposit: row.get(6),
            expansion_rate: row.get(7),
            treasury_growth_rate: row.get(8),
            decentralisation: row.get(9),
            desired_pool_number: row.get(10),
            min_pool_cost: row.get(11),
            influence: row.get(12),
        });
    }

    Ok(pparams)
}

pub(super) fn write_csv(path: &std::path::Path, rows: &[PParamsRow]) -> Result<()> {
    let mut file = std::fs::File::create(path)
        .with_context(|| format!("creating pparams csv: {}", path.display()))?;
    writeln!(
        file,
        "epoch_no,protocol_major,protocol_minor,min_fee_a,min_fee_b,key_deposit,pool_deposit,expansion_rate,treasury_growth_rate,decentralisation,desired_pool_number,min_pool_cost,influence"
    )?;

    for row in rows {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
            row.epoch_no,
            row.protocol_major,
            row.protocol_minor,
            row.min_fee_a,
            row.min_fee_b,
            row.key_deposit,
            row.pool_deposit,
            row.expansion_rate,
            row.treasury_growth_rate,
            row.decentralisation,
            row.desired_pool_number,
            row.min_pool_cost,
            row.influence,
        )?;
    }

    Ok(())
}
