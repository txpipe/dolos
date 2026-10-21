use dolos_cardano::{
    model::AccountState, DRepState, EpochState, GovState, PoolState, ProposalState,
};
use dolos_core::{KeyCodec as _, StateEntity};
use miette::IntoDiagnostic;

use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// namespace of the entity to update
    #[arg(long)]
    namespace: String,

    /// hex key of the entity, for namespaces that hold more than one
    #[arg(long)]
    key: Option<String>,
}

pub fn run_typed<E: StateEntity + std::fmt::Debug, S: StateStore>(
    state: &S,
    args: &Args,
) -> miette::Result<()> {
    // a singleton's key is empty, so it needs no `--key`
    let bytes = hex::decode(args.key.as_deref().unwrap_or_default()).into_diagnostic()?;

    let key = E::Key::decode_key(&bytes).map_err(|err| match args.key {
        Some(_) => miette::miette!("not a {} key: {err}", E::NS),
        None => miette::miette!("--key is required for this namespace"),
    })?;

    let entity = state.read_entity_typed::<E>(&key).into_diagnostic()?;

    if let Some(entity) = entity {
        println!("{:#?}", entity);
    } else {
        println!("entity not found");
    }

    Ok(())
}

use dolos_core::config::RootConfig;

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, config.telemetry.as_ref())?;

    let state = crate::common::open_state_store(config)?;

    match args.namespace.as_str() {
        "epochs" => run_typed::<EpochState, _>(&state, args)?,
        "accounts" => run_typed::<AccountState, _>(&state, args)?,
        "pools" => run_typed::<PoolState, _>(&state, args)?,
        "proposals" => run_typed::<ProposalState, _>(&state, args)?,
        "dreps" => run_typed::<DRepState, _>(&state, args)?,
        "gov" => run_typed::<GovState, _>(&state, args)?,
        _ => return Err(miette::Error::msg("invalid namespace")),
    };

    Ok(())
}
