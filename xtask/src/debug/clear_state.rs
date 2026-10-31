use anyhow::{bail, Context, Result};
use dolos::storage::Access;
use dolos_core::{config::RootConfig, KeyRange, Namespace, StateStore as _, StateWriter as _};
use itertools::Itertools as _;

use super::ConfigArgs;

/// The namespaces `clear-state` empties.
pub const NAMESPACES: [Namespace; 4] = ["eras", "epochs", "accounts", "pools"];

#[derive(clap::Args)]
pub struct Args {
    #[command(flatten)]
    config: ConfigArgs,

    /// namespace to clear
    #[arg(long)]
    namespace: String,
}

pub fn run(args: &Args) -> Result<()> {
    clear_state(&args.config.load()?, &args.namespace)
}

/// Delete every entity of `namespace`, one of [`NAMESPACES`], in one commit.
pub fn clear_state(config: &RootConfig, namespace: &str) -> Result<()> {
    let Some(ns) = NAMESPACES.into_iter().find(|ns| *ns == namespace) else {
        bail!(
            "cannot clear namespace {namespace:?}; supported: {}",
            NAMESPACES.join(", ")
        );
    };

    let state = super::open(config, Access::Mutate)?.state;
    let writer = state.start_writer()?;

    let all_keys = state
        .iter_entities(ns, KeyRange::all())
        .context("iterating entities")?
        .map_ok(|(key, _)| key);

    for key in all_keys {
        writer.delete_entity(ns, &key?)?;
    }

    writer.commit()?;

    Ok(())
}
