use dolos_core::{config::RootConfig, ChainPoint, StateStore, StateWriter};
use miette::{Context, IntoDiagnostic};
use tracing::info;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args, Default, Clone)]
pub struct Args {}

pub fn run(config: &RootConfig, _args: &Args, _feedback: &Feedback) -> miette::Result<()> {
    let state = crate::common::open_state_store(config)?;

    let writer = state
        .start_writer()
        .into_diagnostic()
        .context("opening state writer")?;

    writer
        .set_cursor(ChainPoint::Origin)
        .into_diagnostic()
        .context("setting origin cursor")?;

    writer
        .commit()
        .into_diagnostic()
        .context("committing origin cursor")?;

    info!("data initialized to sync from origin");

    Ok(())
}
