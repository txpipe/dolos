use dolos_cardano::{
    model::AccountState, DRepState, EpochState, GovState, PoolState, ProposalState,
};
use dolos_core::{KeyCodec as _, StateEntity};
use miette::IntoDiagnostic;
use serde::Serialize;

use crate::output::{Format, FormatArgs, Usage};
use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// namespace of the entity
    #[arg(long)]
    namespace: String,

    /// hex key of the entity, for namespaces that hold more than one
    #[arg(long)]
    key: Option<String>,

    #[command(flatten)]
    format: FormatArgs,
}

const NAMESPACES: [&str; 6] = ["epochs", "accounts", "pools", "proposals", "dreps", "gov"];

pub fn run_typed<E: StateEntity + std::fmt::Debug + Serialize, S: StateStore>(
    state: &S,
    args: &Args,
) -> miette::Result<()> {
    // a singleton's key is empty, so it needs no `--key`
    let bytes = hex::decode(args.key.as_deref().unwrap_or_default())
        .map_err(|err| Usage::new(format!("--key is not hex: {err}"), None))?;

    let key = E::Key::decode_key(&bytes).map_err(|err| match args.key {
        Some(_) => Usage::new(format!("not a {} key: {err}", E::NS), None),
        None => Usage::new("--key is required for this namespace", None),
    })?;

    let entity = state.read_entity_typed::<E>(&key).into_diagnostic()?;

    match (args.format.format, entity) {
        (Format::Text, Some(entity)) => println!("{entity:#?}"),
        (Format::Text, None) => println!("entity not found"),
        (Format::Json, entity) => crate::output::emit_record(&entity)?,
    }

    Ok(())
}

use dolos_core::config::RootConfig;

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    match args.format.format {
        Format::Text => crate::common::setup_tracing_warnings()?,
        Format::Json => crate::common::setup_tracing_error_only()?,
    }

    if !NAMESPACES.contains(&args.namespace.as_str()) {
        return Err(Usage::new(
            format!("no state namespace {} to inspect", args.namespace),
            Some(format!("one of: {}", NAMESPACES.join(", "))),
        )
        .into());
    }

    let state = &dolos::storage::open_existing_state(config, dolos::storage::Access::Observe)?;

    match args.namespace.as_str() {
        "epochs" => run_typed::<EpochState, _>(state, args),
        "accounts" => run_typed::<AccountState, _>(state, args),
        "pools" => run_typed::<PoolState, _>(state, args),
        "proposals" => run_typed::<ProposalState, _>(state, args),
        "dreps" => run_typed::<DRepState, _>(state, args),
        "gov" => run_typed::<GovState, _>(state, args),
        _ => unreachable!("checked against NAMESPACES"),
    }
}
