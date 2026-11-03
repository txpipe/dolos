use std::str::FromStr as _;

use dolos::storage::Access;
use dolos_core::{config::RootConfig, sync::SyncExt as _, BlockHash, BlockSlot, ChainPoint};
use miette::{Context, IntoDiagnostic};

use dolos::prelude::*;

use super::Positions;
use crate::output::ConfirmArgs;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// The slot of the block to roll back to
    #[arg(long)]
    slot: BlockSlot,

    /// The hash of the block to roll back to
    #[arg(long, value_parser = parse_hash)]
    hash: BlockHash,

    #[command(flatten)]
    confirm: ConfirmArgs,
}

fn parse_hash(hash: &str) -> Result<BlockHash, String> {
    BlockHash::from_str(hash).map_err(|error| format!("not a block hash: {error}"))
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let point = ChainPoint::Specific(args.slot, args.hash);

    args.confirm.require(&format!(
        "every block after {point} in the WAL, the state and the archive"
    ))?;

    crate::common::setup_tracing_warnings()?;

    let stores = crate::common::open_existing_stores(config, Access::Mutate)?;
    let positions = Positions::read(config, &stores)?;

    // Each undone block takes back what it applied, so a store that never
    // applied it would be unwound past its own position. An archive ahead
    // holds blocks the WAL cannot undo.
    let disagreements = positions.disagreements();
    if !disagreements.is_empty() {
        let help = if positions.only_archive_ahead() {
            "sync past the archive tip with `dolos sync catchup` first"
        } else {
            "run `dolos sync recover` first"
        };

        miette::bail!(
            help = help,
            "the stores disagree: {}",
            disagreements.join("; ")
        );
    }

    // The all-zero point names the WAL's Origin entry, which holds no block
    // to roll back to.
    let held = point.clone().into_bytes() != ChainPoint::Origin.into_bytes()
        && stores
            .wal
            .contains_point(&point)
            .into_diagnostic()
            .context("looking the point up in the WAL")?;

    if !held {
        let window = match (&positions.rollback_floor, &positions.wal_tip) {
            (Some(floor), Some(tip)) => {
                format!("slot {} to slot {}", floor.point.slot(), tip.slot())
            }
            _ => "nothing past Origin".to_string(),
        };

        miette::bail!(
            help = "`dolos sync dump` lists the points the WAL holds",
            "{point} is not in the WAL, which holds {window}: a rollback reaches only a point it holds",
        );
    }

    let domain = crate::common::assemble_domain(config, &stores)?;

    domain
        .rollback(&point)
        .into_diagnostic()
        .context("rolling back")?;

    domain
        .shutdown()
        .into_diagnostic()
        .context("closing the stores")?;

    println!("rolled the WAL, the state and the archive back to {point}");

    Ok(())
}
