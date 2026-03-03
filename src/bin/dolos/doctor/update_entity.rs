use dolos_cardano::{model::AccountState, EpochState, FixedNamespace as _, PoolState};
use dolos_core::config::RootConfig;
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

    /// cbor hex of the new entity value
    #[arg(long)]
    cbor_hex: String,

    /// whether to dry run the update
    #[arg(long, action)]
    execute: bool,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    let state = crate::common::open_state_store(config)?;

    let ns = match args.namespace.as_str() {
        "epochs" => EpochState::NS,
        "accounts" => AccountState::NS,
        "pools" => PoolState::NS,
        _ => return Err(miette::Error::msg("invalid namespace")),
    };

    let key = hex::decode(&args.key).into_diagnostic()?;
    let key = EntityKey::from(key);

    let value = hex::decode(&args.cbor_hex).into_diagnostic()?;
    let value = EntityValue::from(value);

    let query = state.read_entities(ns, &[&key]).into_diagnostic()?;

    let old_value = query.first().unwrap();

    if let Some(old_value) = old_value {
        println!("old value: {}", hex::encode(old_value));
    }

    let writer = state.start_writer().into_diagnostic()?;

    writer.write_entity(ns, &key, &value).into_diagnostic()?;

    if args.execute {
        writer.commit().into_diagnostic()?;
    }

    Ok(())
}
