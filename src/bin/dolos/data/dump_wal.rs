use comfy_table::Table;
use dolos_cardano::CardanoDelta;
use dolos_core::config::RootConfig;
use miette::{Context, IntoDiagnostic};

use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// start the dump from this sequence number
    #[arg(long)]
    from: Option<u64>,

    /// only dump this amount of WAL entries
    #[arg(long, default_value = "100")]
    limit: usize,
}

enum Formatter {
    Table(Table),
    // TODO
    // Json,
}

impl Formatter {
    fn new_table() -> Self {
        let mut table = Table::new();
        table.set_header(vec!["Slot", "Hash", "Block Size", "State Deltas", "Inputs"]);

        Self::Table(table)
    }

    fn write(&mut self, point: &ChainPoint, log: &LogValue<CardanoDelta>) {
        match self {
            Formatter::Table(table) => {
                let slot = point.slot().to_string();

                let hash = point
                    .hash()
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or("none".into());

                let size = log.block.len().to_string();
                let deltas = log.delta.len().to_string();
                let inputs = log.inputs.len().to_string();

                table.add_row(vec![
                    format!("{slot}"),
                    format!("{hash}"),
                    format!("{size}"),
                    format!("{deltas}"),
                    format!("{inputs}"),
                ]);
            }
        }
    }

    fn flush(self) {
        match self {
            Formatter::Table(table) => println!("{table}"),
        }
    }
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let wal = crate::common::open_wal_store(config)?;

    let mut formatter = Formatter::new_table();

    let from = match args.from {
        Some(slot) => {
            let point = wal
                .locate_point(slot)
                .into_diagnostic()?
                .ok_or(miette::miette!("slot not found"))?;

            Some(point)
        }
        None => None,
    };

    wal.iter_logs(from, None)
        .into_diagnostic()
        .context("crawling wal")?
        .take(args.limit)
        .for_each(|(point, log)| formatter.write(&point, &log));

    formatter.flush();

    Ok(())
}
