use dolos_core::config::RootConfig;
use dolos_core::WalStore;
use miette::{Context, IntoDiagnostic};
use tracing::info;

use dolos::prelude::*;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: &RootConfig, _args: &Args, _feedback: &Feedback) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    let state = crate::common::open_state_store(config)?;
    let wal = crate::common::open_wal_store(config)?;

    let cursor = state
        .read_cursor()
        .into_diagnostic()
        .context("getting state cursor")?;

    let Some(cursor) = cursor else {
        return Err(miette::miette!("state has no cursor, nothing to reset to"));
    };

    if !cursor.is_fully_defined() {
        return Err(miette::miette!(
            help = "re-bootstrap to get a valid state with a block hash",
            "state cursor at slot {} is not fully defined (missing block hash)",
            cursor.slot(),
        ));
    }

    wal.reset_to(&cursor)
        .into_diagnostic()
        .context("resetting wal")?;

    info!(%cursor, "WAL reset successfully");

    Ok(())
}
