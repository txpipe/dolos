//! Epoch dataset: fetch from DBSync and write to CSV.

use anyhow::{Context, Result};
use std::io::Write;

pub(super) struct EpochRow {
    pub epoch_no: i64,
    pub protocol_major: i32,
    pub treasury: String,
    pub reserves: String,
    pub rewards: String,
    pub utxo: String,
    pub deposits_stake: String,
    pub fees: String,
    pub nonce: String,
    pub block_count: String,
}

/// Fetch epoch states from DBSync up to (and including) `max_epoch`.
pub(super) fn fetch(dbsync_url: &str, max_epoch: u64) -> Result<Vec<EpochRow>> {
    let mut client = super::connect_to_dbsync(dbsync_url)?;

    let query = r#"
        SELECT
            e.no::bigint AS epoch_no,
            ep.protocol_major AS protocol_major,
            ep.nonce AS epoch_nonce,
            ap.treasury::text AS treasury,
            ap.reserves::text AS reserves,
            ap.rewards::text AS rewards,
            ap.utxo::text AS utxo,
            ap.deposits_stake::text AS deposits_stake,
            ap.fees::text AS fees_pot,
            e.blk_count::text AS block_count
        FROM epoch e
        JOIN epoch_param ep ON ep.epoch_no = e.no
        LEFT JOIN ada_pots ap ON ap.epoch_no = e.no
        WHERE e.no >= 1 AND e.no <= $1
        ORDER BY e.no
    "#;

    let max_epoch = i32::try_from(max_epoch)
        .map_err(|_| anyhow::anyhow!("max_epoch out of range for dbsync (expected i32)"))?;
    let rows = client
        .query(query, &[&max_epoch])
        .with_context(|| "Failed to query epoch states")?;

    let mut epochs = Vec::new();

    for row in rows {
        let epoch_no: i64 = row.get(0);
        let protocol_major: i32 = row.get(1);
        let epoch_nonce: Option<Vec<u8>> = row.get(2);
        let treasury: Option<String> = row.get(3);
        let reserves: Option<String> = row.get(4);
        let rewards: Option<String> = row.get(5);
        let utxo: Option<String> = row.get(6);
        let deposits_stake: Option<String> = row.get(7);
        let fees: Option<String> = row.get(8);
        let block_count: Option<String> = row.get(9);

        let nonce = epoch_nonce
            .map(|b| hex::encode(&b))
            .unwrap_or_default();

        epochs.push(EpochRow {
            epoch_no,
            protocol_major,
            treasury: treasury.unwrap_or_else(|| "0".into()),
            reserves: reserves.unwrap_or_else(|| "0".into()),
            rewards: rewards.unwrap_or_else(|| "0".into()),
            utxo: utxo.unwrap_or_else(|| "0".into()),
            deposits_stake: deposits_stake.unwrap_or_else(|| "0".into()),
            fees: fees.unwrap_or_else(|| "0".into()),
            nonce,
            block_count: block_count.unwrap_or_else(|| "0".into()),
        });
    }

    Ok(epochs)
}

pub(super) fn write_csv(path: &std::path::Path, epochs: &[EpochRow]) -> Result<()> {
    let mut file = std::fs::File::create(path)
        .with_context(|| format!("creating epochs csv: {}", path.display()))?;
    writeln!(
        file,
        "epoch_no,protocol_major,treasury,reserves,rewards,utxo,deposits_stake,fees,nonce,block_count"
    )?;

    for epoch in epochs {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{}",
            epoch.epoch_no,
            epoch.protocol_major,
            epoch.treasury,
            epoch.reserves,
            epoch.rewards,
            epoch.utxo,
            epoch.deposits_stake,
            epoch.fees,
            epoch.nonce,
            epoch.block_count
        )?;
    }

    Ok(())
}
