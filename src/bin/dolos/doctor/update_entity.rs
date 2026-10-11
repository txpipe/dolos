use dolos_cardano::{model::AccountState, EpochState, PoolState};
use dolos_core::{config::RootConfig, KeyCodec as _, StateEntity};
use miette::IntoDiagnostic;

use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// namespace of the entity to update
    #[arg(long)]
    namespace: String,

    /// hex key of the entity to update; empty for a singleton
    #[arg(long, default_value = "")]
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

    let (ns, key) = match args.namespace.as_str() {
        "epochs" => typed_key::<EpochState>(&args.key)?,
        "accounts" => typed_key::<AccountState>(&args.key)?,
        "pools" => typed_key::<PoolState>(&args.key)?,
        _ => return Err(miette::Error::msg("invalid namespace")),
    };

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

/// The namespace of `E` and the stored key `hex` names in it, refused unless
/// it decodes exactly as `E`'s key.
fn typed_key<E: StateEntity>(hex: &str) -> miette::Result<(Namespace, EntityKey)> {
    let bytes = hex::decode(hex).into_diagnostic()?;

    let key =
        E::Key::decode_key(&bytes).map_err(|err| miette::miette!("not a {} key: {err}", E::NS))?;

    Ok((E::NS, EntityKey::of(&key)))
}
