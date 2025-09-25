use itertools::Itertools as _;
use miette::{Context, IntoDiagnostic};

use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// namespace to dump
    #[arg(long)]
    namespace: String,
}

fn clear_state(config: &crate::Config, ns: Namespace) -> miette::Result<()> {
    let state = crate::common::open_state_store(config)?;
    let writer = state.start_writer().into_diagnostic()?;

    let all_keys = state
        .iter_entities(ns, EntityKey::full_range())
        .into_diagnostic()
        .context("iterating entities")?
        .map_ok(|(key, _)| key);

    for key in all_keys {
        let key = key.into_diagnostic()?;
        writer.delete_entity(ns, &key).into_diagnostic()?;
    }

    writer.commit().into_diagnostic()?;

    Ok(())
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    match args.namespace.as_str() {
        "eras" => clear_state(config, "eras")?,
        "epochs" => clear_state(config, "epochs")?,
        "accounts" => clear_state(config, "accounts")?,
        "pools" => clear_state(config, "pools")?,
        _ => todo!(),
    }

    Ok(())
}
