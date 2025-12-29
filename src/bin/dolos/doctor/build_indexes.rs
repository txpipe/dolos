use std::{sync::Arc};

use dolos_core::config::RootConfig;
use itertools::Itertools;
use miette::{Context, IntoDiagnostic};

use dolos::prelude::*;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(short, long, default_value_t = 500)]
    pub chunk: usize,
}

#[tokio::main]
pub async fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    //crate::common::setup_tracing(&config.logging)?;

    let progress = feedback.slot_progress_bar();
    progress.set_message("building indexes");

    let mut domain = crate::common::setup_domain(config).await?;

    progress.set_length(domain.state.amount_of_utxos().into_diagnostic().context("getting amount of utxos")?);

    let remaining = domain
        .state
        .iter_utxos()
        .into_diagnostic()
        .context("iterating over utxos")?;

    for chunk in remaining.chunks(args.chunk).into_iter() {
        let produced_utxo = chunk.into_iter().map(|x| {
            let (k, v) = x.into_diagnostic().context("decoding utxoset")?;
            Ok((k, Arc::new(v)))
        }).collect::<miette::Result<_>>()?;
        let utxoset = UtxoSetDelta {produced_utxo, ..Default::default()};

        { 
            let writer = domain.state.start_writer().into_diagnostic().context("starting writer")?;
            writer.index_utxoset(&utxoset).into_diagnostic().context("indexing")?;
            writer.commit().into_diagnostic().context("committing")?; 
        }

        progress.inc(args.chunk as u64);
    }

    let db = domain.state.db_mut().unwrap();

    progress.set_message("compacting");
    db.compact().into_diagnostic().context("compacting")?;

    progress.set_message("checking integrity");
    db.check_integrity().into_diagnostic().context("checking integrity")?;

    Ok(())
}
