use dolos_cardano::{model::AccountState, EpochState, FixedNamespace, PoolState, ProposalState};
use miette::IntoDiagnostic;

use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// namespace of the entity to update
    #[arg(long)]
    namespace: String,

    /// key of the entity to update
    #[arg(long)]
    key: String,
}

pub fn run_typed<E: Entity + FixedNamespace + std::fmt::Debug, S: StateStore>(
    state: &S,
    args: &Args,
) -> miette::Result<()> {
    let key = hex::decode(&args.key).into_diagnostic()?;
    let key = EntityKey::from(key);

    let entity = state
        .read_entity_typed::<E>(E::NS, &key)
        .into_diagnostic()?;

    if let Some(entity) = entity {
        println!("{:#?}", entity);
    } else {
        println!("entity not found");
    }

    Ok(())
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let state = crate::common::open_state_store(config)?;

    match args.namespace.as_str() {
        "epochs" => run_typed::<EpochState, _>(&state, args)?,
        "accounts" => run_typed::<AccountState, _>(&state, args)?,
        "pools" => run_typed::<PoolState, _>(&state, args)?,
        "proposals" => run_typed::<ProposalState, _>(&state, args)?,
        _ => return Err(miette::Error::msg("invalid namespace")),
    };

    Ok(())
}
