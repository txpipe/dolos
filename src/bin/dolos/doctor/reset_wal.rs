use std::collections::HashMap;

use miette::{Context, IntoDiagnostic};

use dolos::prelude::*;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &crate::Config, _args: &Args, feedback: &Feedback) -> miette::Result<()> {
    //crate::common::setup_tracing(&config.logging)?;

    let progress = feedback.slot_progress_bar();
    progress.set_message("rebuilding stores");

    let domain = crate::common::setup_domain(config)?;

    let cursor = domain
        .state
        .read_cursor()
        .into_diagnostic()
        .context("getting state cursor")?;

    let Some(cursor) = cursor else {
        return Err(miette::miette!("state has no cursor"));
    };

    let entry = (
        cursor,
        LogValue {
            block: vec![],
            delta: vec![],
            inputs: HashMap::new(),
        },
    );

    domain
        .wal()
        .append_entries(&vec![entry])
        .into_diagnostic()
        .context("appending wal entry")?;

    Ok(())
}
