//! Era dataset: fetch from DBSync and write to CSV.

use anyhow::{Context, Result};
use std::io::Write;

use crate::config::Network;

pub(super) struct EraRow {
    pub protocol: u16,
    pub start_epoch: u64,
    pub epoch_length: u64,
    pub slot_length: u64,
}

/// Fetch era summaries from DBSync up to (and including) the era containing `max_epoch`.
pub(super) fn fetch(dbsync_url: &str, max_epoch: u64, network: &Network) -> Result<Vec<EraRow>> {
    let mut client = super::connect_to_dbsync(dbsync_url)?;

    let query = r#"
        SELECT
            e.no::bigint AS epoch_no,
            EXTRACT(EPOCH FROM e.start_time)::bigint AS start_time,
            ep.protocol_major::int4
        FROM epoch e
        JOIN epoch_param ep ON ep.epoch_no = e.no
        WHERE e.no <= $1
        ORDER BY e.no
    "#;

    let max_epoch = i32::try_from(max_epoch)
        .map_err(|_| anyhow::anyhow!("max_epoch out of range for dbsync (expected i32)"))?;
    let rows = client
        .query(query, &[&max_epoch])
        .with_context(|| "Failed to query epoch boundaries")?;

    let mut ordered_epochs: Vec<(u64, u64, u16)> = Vec::new();

    for row in rows {
        let epoch_no: i64 = row.get(0);
        let start_time: i64 = row.get(1);
        let protocol_major: i32 = row.get(2);

        ordered_epochs.push((epoch_no as u64, start_time as u64, protocol_major as u16));
    }

    ordered_epochs.sort_by_key(|(epoch, _, _)| *epoch);

    let mut era_starts: Vec<(u16, u64)> = Vec::new();
    let mut last_protocol: Option<u16> = None;

    for (epoch, _timestamp, protocol) in &ordered_epochs {
        if last_protocol.map(|p| p != *protocol).unwrap_or(true) {
            era_starts.push((*protocol, *epoch));
            last_protocol = Some(*protocol);
        }
    }

    // Skip Byron eras (protocol < 2) — no reliable DBSync ground truth
    era_starts.retain(|(protocol, _)| *protocol >= 2);

    let mut eras = Vec::new();

    for (protocol, epoch) in &era_starts {
        let epoch_length = match network {
            Network::Mainnet => 432000,
            Network::Preview | Network::Preprod => 86400,
        };
        let (epoch_length, slot_length) = (epoch_length, 1);

        eras.push(EraRow {
            protocol: *protocol,
            start_epoch: *epoch,
            epoch_length,
            slot_length,
        });
    }

    Ok(eras)
}

pub(super) fn write_csv(path: &std::path::Path, eras: &[EraRow]) -> Result<()> {
    let mut file = std::fs::File::create(path)
        .with_context(|| format!("creating eras csv: {}", path.display()))?;
    writeln!(file, "protocol,start_epoch,epoch_length,slot_length")?;

    for era in eras {
        writeln!(
            file,
            "{},{},{},{}",
            era.protocol, era.start_epoch, era.epoch_length, era.slot_length
        )?;
    }

    Ok(())
}
