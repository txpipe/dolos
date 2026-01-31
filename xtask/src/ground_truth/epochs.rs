//! Epoch dataset: fetch from DBSync and write to CSV.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

pub(super) struct EpochRow {
    pub epoch_no: i64,
    pub treasury: String,
    pub reserves: String,
    pub rewards: String,
    pub utxo: String,
    pub deposits_stake: String,
    pub fees: String,
    pub nonce: String,
}

/// Fetch epoch states from DBSync up to (and including) `max_epoch`.
pub(super) fn fetch(dbsync_url: &str, max_epoch: u64) -> Result<Vec<EpochRow>> {
    let mut client = super::connect_to_dbsync(dbsync_url)?;

    let query = r#"
        SELECT
            e.no::bigint AS epoch_no,
            ep.nonce AS epoch_nonce,
            ap.treasury::text AS treasury,
            ap.reserves::text AS reserves,
            ap.rewards::text AS rewards,
            ap.utxo::text AS utxo,
            ap.deposits_stake::text AS deposits_stake,
            ap.fees::text AS fees_pot
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
        let epoch_nonce: Option<Vec<u8>> = row.get(1);
        let treasury: Option<String> = row.get(2);
        let reserves: Option<String> = row.get(3);
        let rewards: Option<String> = row.get(4);
        let utxo: Option<String> = row.get(5);
        let deposits_stake: Option<String> = row.get(6);
        let fees: Option<String> = row.get(7);

        let nonce = epoch_nonce
            .map(|b| hex::encode(&b))
            .unwrap_or_default();

        epochs.push(EpochRow {
            epoch_no,
            treasury: treasury.unwrap_or_else(|| "0".into()),
            reserves: reserves.unwrap_or_else(|| "0".into()),
            rewards: rewards.unwrap_or_else(|| "0".into()),
            utxo: utxo.unwrap_or_else(|| "0".into()),
            deposits_stake: deposits_stake.unwrap_or_else(|| "0".into()),
            fees: fees.unwrap_or_else(|| "0".into()),
            nonce,
        });
    }

    Ok(epochs)
}

pub(super) fn write_csv(path: &std::path::Path, epochs: &[EpochRow]) -> Result<()> {
    let mut file = std::fs::File::create(path)
        .with_context(|| format!("creating epochs csv: {}", path.display()))?;
    writeln!(
        file,
        "epoch_no,treasury,reserves,rewards,utxo,deposits_stake,fees,nonce"
    )?;

    for epoch in epochs {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{}",
            epoch.epoch_no,
            epoch.treasury,
            epoch.reserves,
            epoch.rewards,
            epoch.utxo,
            epoch.deposits_stake,
            epoch.fees,
            epoch.nonce
        )?;
    }

    Ok(())
}

pub(super) fn dump_dolos_csv(config_path: &Path, epoch: u64, output_path: &Path) -> Result<()> {
    let file = File::create(output_path)
        .with_context(|| format!("writing epochs csv: {}", output_path.display()))?;

    let status = Command::new("cargo")
        .arg("run")
        .arg("-p")
        .arg("dolos")
        .arg("--features")
        .arg("utils")
        .arg("--")
        .arg("data")
        .arg("dump-logs")
        .arg("--namespace")
        .arg("epochs")
        .arg("--format")
        .arg("dbsync")
        .arg("--epoch-start")
        .arg("1")
        .arg("--epoch-end")
        .arg(epoch.to_string())
        .arg("--take")
        .arg("0")
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::from(file))
        .stderr(Stdio::inherit())
        .status()
        .context("running dolos dump-logs for epochs")?;

    if !status.success() {
        anyhow::bail!("dolos dump-logs failed for epochs");
    }

    Ok(())
}
