use comfy_table::Table;
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
        table.set_header(vec!["Seq", "Event", "Slot", "Hash", "Era", "Block Size"]);

        Self::Table(table)
    }

    fn write(&mut self, seq: LogSeq, value: LogValue) {
        let (evt, slot, hash, era, size) = match value {
            LogValue::Apply(block) => {
                let RawBlock {
                    slot,
                    hash,
                    era,
                    body,
                } = block;

                ("apply", Some(slot), Some(hash), Some(era), Some(body.len()))
            }
            LogValue::Undo(block) => {
                let RawBlock {
                    slot,
                    hash,
                    era,
                    body,
                } = block;

                ("undo", Some(slot), Some(hash), Some(era), Some(body.len()))
            }
            LogValue::Mark(ChainPoint::Specific(slot, hash)) => {
                ("mark", Some(slot), Some(hash), None, None)
            }
            LogValue::Mark(ChainPoint::Origin) => ("origin", None, None, None, None),
        };

        match self {
            Formatter::Table(table) => {
                let slot = slot
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or("none".into());

                let hash = hash
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or("none".into());

                let era = era
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or("none".into());

                let size = size
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or("none".into());

                table.add_row(vec![
                    format!("{seq}"),
                    format!("{evt}"),
                    format!("{slot}"),
                    format!("{hash}"),
                    format!("{era}"),
                    format!("{size}"),
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

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(config)?;

    let wal = crate::common::open_wal_store(config)?;

    let mut formatter = Formatter::new_table();

    wal.crawl_from(args.from)
        .into_diagnostic()
        .context("crawling wal")?
        .take(args.limit)
        .for_each(|(seq, value)| formatter.write(seq, value));

    formatter.flush();

    Ok(())
}
