use comfy_table::Table;
use dolos::storage::Access;
use dolos_core::{config::RootConfig, BlockSlot};
use miette::{Context, IntoDiagnostic};
use serde::Serialize;

use dolos::prelude::*;

use crate::output::{emit_record, Format, FormatArgs};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// Start at the WAL entry at, or nearest to, this slot
    #[arg(long, value_name = "SLOT")]
    start: Option<BlockSlot>,

    /// How many entries to print; 0 prints them all
    #[arg(long, value_name = "N", default_value_t = 100)]
    limit: usize,

    #[command(flatten)]
    format: FormatArgs,
}

/// The rows of one text table, when every entry is printed.
const TABLE_PAGE: usize = 1000;

/// One WAL entry, as a row of the table or one JSON line.
#[derive(Debug, Serialize)]
struct Entry {
    slot: BlockSlot,
    /// `none` for an entry without a block hash.
    hash: String,
    block_size: usize,
    state_deltas: usize,
    inputs: usize,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing_warnings()?;

    let stores = crate::common::open_existing_stores(config, Access::Observe)?;
    let wal = &stores.wal;

    let start = match args.start {
        Some(slot) => Some(
            wal.locate_point(slot)
                .into_diagnostic()?
                .ok_or(miette::miette!("no WAL entry near slot {slot}"))?,
        ),
        None => None,
    };

    let limit = match args.limit {
        0 => usize::MAX,
        limit => limit,
    };

    let entries = wal
        .iter_logs(start, None)
        .into_diagnostic()
        .context("crawling wal")?
        .take(limit)
        .map(|(point, log)| Entry {
            slot: point.slot(),
            hash: point
                .hash()
                .as_ref()
                .map_or("none".into(), ToString::to_string),
            block_size: log.block.len(),
            state_deltas: log.delta.len(),
            inputs: log.inputs.len(),
        });

    match args.format.format {
        Format::Json => {
            for entry in entries {
                emit_record(&entry)?;
            }
        }
        Format::Text => {
            // A dump of every entry prints a table per page, so it streams
            // rather than holding each row until the last; a bounded dump is
            // one table.
            let page = match args.limit {
                0 => TABLE_PAGE,
                limit => limit,
            };

            let mut entries = entries.peekable();

            loop {
                let mut table = Table::new();
                table.set_header(vec!["Slot", "Hash", "Block Size", "State Deltas", "Inputs"]);

                for entry in entries.by_ref().take(page) {
                    table.add_row(vec![
                        entry.slot.to_string(),
                        entry.hash,
                        entry.block_size.to_string(),
                        entry.state_deltas.to_string(),
                        entry.inputs.to_string(),
                    ]);
                }

                println!("{table}");

                if entries.peek().is_none() {
                    break;
                }
            }
        }
    }

    Ok(())
}
