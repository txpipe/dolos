use dolos::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSummary {
    pub start_seq: Option<LogSeq>,
    pub start_slot: Option<BlockSlot>,
    pub tip_seq: Option<LogSeq>,
    pub tip_slot: Option<BlockSlot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveSummary {
    // TODO: archive interface needs a way to query from the start
    // pub start: Option<ChainPoint>,
    pub tip_slot: Option<BlockSlot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSummary {
    pub tip_slot: Option<BlockSlot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub wal: WalSummary,
    pub archive: ArchiveSummary,
    pub state: StateSummary,
}

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
        tip_slot: state.cursor().unwrap().map(|x| x.slot()),
    };

    let summary = Summary {
        wal: wal_summary,
        archive: archive_summary,
        state: state_summary,
    };

    println!("{}", serde_json::to_string_pretty(&summary).unwrap());

    Ok(())
}
