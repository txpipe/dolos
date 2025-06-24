use dolos::cli::{ArchiveSummary, DataSummary, StateSummary, WalSummary};
use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &crate::Config, _args: &Args) -> miette::Result<()> {
    let (wal, state, archive) = crate::common::setup_data_stores(config)?;

    let wal_start = wal.crawl_from(None).unwrap().next();
    let wal_tip = wal.find_tip().unwrap();

    let wal_summary = WalSummary {
        start_seq: wal_start.as_ref().map(|(seq, _)| *seq),
        start_slot: wal_start.as_ref().map(|(_, x)| x.slot()),
        tip_seq: wal_tip.as_ref().map(|(seq, _)| *seq),
        tip_slot: wal_tip.as_ref().map(|(_, x)| x.slot()),
    };

    let archive_summary = ArchiveSummary {
        tip_slot: archive.get_tip().unwrap().map(|(slot, _)| slot),
    };

    let state_summary = StateSummary {
        start_slot: state.start().unwrap().map(|x| x.slot()),
        tip_slot: state.cursor().unwrap().map(|x| x.slot()),
    };

    let summary = DataSummary {
        wal: wal_summary,
        archive: archive_summary,
        state: state_summary,
    };

    println!("{}", serde_json::to_string_pretty(&summary).unwrap());

    Ok(())
}
