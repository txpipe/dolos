use dolos_core::config::RootConfig;
use dolos::cli::{ArchiveSummary, DataSummary, StateSummary, WalSummary};
use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &RootConfig, _args: &Args) -> miette::Result<()> {
    let stores = crate::common::setup_data_stores(config)?;

    let wal_start = stores.wal.find_start().unwrap();
    let wal_tip = stores.wal.find_tip().unwrap();

    let wal_summary = WalSummary {
        start_slot: wal_start.as_ref().map(|(x, _)| x.slot()),
        tip_slot: wal_tip.as_ref().map(|(x, _)| x.slot()),
    };

    let archive_summary = ArchiveSummary {
        tip_slot: stores.archive.get_tip().unwrap().map(|(slot, _)| slot),
    };

    let state_summary = StateSummary {
        tip_slot: stores.state.read_cursor().unwrap().map(|x| x.slot()),
    };

    let summary = DataSummary {
        wal: wal_summary,
        archive: archive_summary,
        state: state_summary,
    };

    println!("{}", serde_json::to_string_pretty(&summary).unwrap());

    Ok(())
}
