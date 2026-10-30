use comfy_table::Table;
use dolos_cardano::owned::OwnedMultiEraBlock;
use dolos_core::config::RootConfig;
use miette::{Context as _, IntoDiagnostic as _};
use serde::Serialize;
use std::sync::Arc;

use super::Bounds;
use crate::output::{emit_record, Format, FormatArgs};
use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// the first slot to dump
    #[arg(long, value_name = "SLOT")]
    from_slot: Option<u64>,

    /// the last slot to dump
    #[arg(long, value_name = "SLOT")]
    to_slot: Option<u64>,

    #[command(flatten)]
    bounds: Bounds,

    #[command(flatten)]
    format: FormatArgs,
}

/// One block, as a table row or a JSON line.
#[derive(Serialize)]
struct Row {
    slot: BlockSlot,
    number: u64,
    hash: String,
}

impl Row {
    fn header() -> Vec<&'static str> {
        vec!["slot", "number", "hash"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.slot.to_string(),
            self.number.to_string(),
            self.hash.clone(),
        ]
    }
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    match args.format.format {
        Format::Text => crate::common::setup_tracing_warnings()?,
        Format::Json => crate::common::setup_tracing_error_only()?,
    }

    let archive = super::observe_archive(config)?;

    // `get_range` ends before its `to`.
    let to = args.to_slot.map(|slot| slot.saturating_add(1));

    let blocks = archive
        .get_range(args.from_slot, to)
        .into_diagnostic()
        .context("iterating blocks")?;

    let mut table = Table::new();
    table.set_header(Row::header());

    for (_, body) in args.bounds.apply(blocks) {
        let block = OwnedMultiEraBlock::decode(Arc::new(body))
            .into_diagnostic()
            .context("decoding a block")?;

        let row = Row {
            slot: block.slot(),
            number: block.view().number(),
            hash: block.hash().to_string(),
        };

        match args.format.format {
            Format::Text => {
                table.add_row(row.cells());
            }
            Format::Json => emit_record(&row)?,
        }
    }

    if args.format.format == Format::Text {
        println!("{table}");
    }

    Ok(())
}
