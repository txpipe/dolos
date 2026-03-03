use comfy_table::Table;
use dolos_cardano::owned::OwnedMultiEraBlock;
use dolos_core::config::RootConfig;
use std::sync::Arc;

use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// slot number to start from
    #[arg(long)]
    from: Option<u64>,

    /// slot number to end at
    #[arg(long)]
    to: Option<u64>,
}

trait TableRow {
    fn header() -> Vec<&'static str>;
    fn row(&self) -> Vec<String>;
}

impl TableRow for OwnedMultiEraBlock {
    fn header() -> Vec<&'static str> {
        vec!["slot", "number", "hash"]
    }

    fn row(&self) -> Vec<String> {
        vec![
            format!("{}", self.slot()),
            format!("{}", self.view().number()),
            format!("{}", self.hash()),
        ]
    }
}

enum Formatter {
    Table(Table),
    // TODO
    // Json,
}

impl Formatter {
    fn new_table() -> Self {
        let mut table = Table::new();
        table.set_header(OwnedMultiEraBlock::header());

        Self::Table(table)
    }

    fn write(&mut self, value: &OwnedMultiEraBlock) {
        match self {
            Formatter::Table(table) => {
                let row = value.row();
                table.add_row(row);
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
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    let archive = crate::common::open_archive_store(config)?;

    let mut formatter = Formatter::new_table();

    let blocks = archive.get_range(args.from, args.to).unwrap();

    let mut last_num: Option<u64> = None;

    for (_, block) in blocks {
        let block = OwnedMultiEraBlock::decode(Arc::new(block)).unwrap();

        if let Some(last_num) = last_num {
            if block.view().number() != last_num + 1 {
                panic!("block number is not consecutive");
            }
        }

        formatter.write(&block);

        last_num = Some(block.view().number());
    }

    formatter.flush();

    Ok(())
}
