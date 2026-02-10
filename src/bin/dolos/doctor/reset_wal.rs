use dolos_core::config::RootConfig;
use miette::{Context, IntoDiagnostic};

use dolos::prelude::*;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: &RootConfig, _args: &Args, _feedback: &Feedback) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    let domain = crate::common::setup_domain(config)?;

    let cursor = domain
        .state
        .read_cursor()
        .into_diagnostic()
        .context("getting state cursor")?;

    let Some(cursor) = cursor else {
        return Err(miette::miette!("state has no cursor"));
    };

    domain
        .wal()
        .reset_to(&cursor)
        .into_diagnostic()
        .context("resetting wal")?;

    Ok(())
}
