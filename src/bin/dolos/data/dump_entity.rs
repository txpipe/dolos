use dolos_cardano::{
    model::AccountState, DRepState, EpochState, FixedNamespace, GovState, PoolState, ProposalState,
    SingletonEntity,
};
use miette::IntoDiagnostic;

use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// namespace of the entity to update
    #[arg(long)]
    namespace: String,

    /// key of the entity to update, for namespaces that hold more than one
    #[arg(long)]
    key: Option<String>,
}

pub fn run_typed<E: Entity + FixedNamespace + std::fmt::Debug, S: StateStore>(
    state: &S,
    args: &Args,
) -> miette::Result<()> {
    let key = args
        .key
        .as_deref()
        .ok_or_else(|| miette::Error::msg("--key is required for this namespace"))?;

    let key = hex::decode(key).into_diagnostic()?;
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

use dolos_core::config::RootConfig;

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    let state = crate::common::open_state_store(config)?;

    match args.namespace.as_str() {
        "epochs" => run_typed::<EpochState, _>(&state, args)?,
        "accounts" => run_typed::<AccountState, _>(&state, args)?,
        "pools" => run_typed::<PoolState, _>(&state, args)?,
        "proposals" => run_typed::<ProposalState, _>(&state, args)?,
        "dreps" => run_typed::<DRepState, _>(&state, args)?,
        // the governance singleton has one key and it isn't the caller's to
        // remember, so `--key` is neither needed nor read here
        "gov" => {
            let entity = state
                .read_entity_typed::<GovState>(GovState::NS, &GovState::singleton_key())
                .into_diagnostic()?;

            match entity {
                Some(entity) => println!("{entity:#?}"),
                None => println!("entity not found"),
            }
        }
        _ => return Err(miette::Error::msg("invalid namespace")),
    };

    Ok(())
}
