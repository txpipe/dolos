use anyhow::{anyhow, bail, Result};
use dolos::storage::Access;
use dolos_cardano::{
    model::{AccountState, DRepState},
    EpochState, PoolState,
};
use dolos_core::{
    config::RootConfig, EntityKey, EntityValue, KeyCodec as _, Namespace, StateEntity,
    StateStore as _, StateWriter as _,
};

use super::ConfigArgs;

#[derive(clap::Args)]
pub struct Args {
    #[command(flatten)]
    config: ConfigArgs,

    /// namespace of the entity to update
    #[arg(long)]
    namespace: String,

    /// hex key of the entity to update, for namespaces that hold more than
    /// one
    #[arg(long)]
    key: Option<String>,

    /// cbor hex of the new entity value
    #[arg(long)]
    cbor_hex: String,

    /// print the current value and write nothing
    #[arg(long)]
    dry_run: bool,
}

pub fn run(args: &Args) -> Result<()> {
    let old = update_entity(
        &args.config.load()?,
        &args.namespace,
        args.key.as_deref(),
        &args.cbor_hex,
        args.dry_run,
    )?;

    if let Some(old) = old {
        println!("old value: {}", hex::encode(old));
    }

    if args.dry_run {
        println!("dry run; nothing written");
    }

    Ok(())
}

/// Replace the entity `key` names in `namespace` with `cbor_hex`, unless
/// `dry_run`, returning the value it held.
pub fn update_entity(
    config: &RootConfig,
    namespace: &str,
    key: Option<&str>,
    cbor_hex: &str,
    dry_run: bool,
) -> Result<Option<EntityValue>> {
    let (ns, key) = match namespace {
        "epochs" => typed_key::<EpochState>(key)?,
        "accounts" => typed_key::<AccountState>(key)?,
        "pools" => typed_key::<PoolState>(key)?,
        "dreps" => typed_key::<DRepState>(key)?,
        _ => bail!("invalid namespace"),
    };

    let value = EntityValue::from(hex::decode(cbor_hex)?);

    let access = if dry_run {
        Access::Observe
    } else {
        Access::Mutate
    };

    let state = super::open_state(config, access)?;

    let old = state
        .read_entities(ns, &[&key])?
        .into_iter()
        .next()
        .flatten();

    if !dry_run {
        let writer = state.start_writer()?;
        writer.write_entity(ns, &key, &value)?;
        writer.commit()?;
    }

    Ok(old)
}

/// The namespace of `E` and the stored key `hex` names in it, refused unless
/// it decodes exactly as `E`'s key.
fn typed_key<E: StateEntity>(hex: Option<&str>) -> Result<(Namespace, EntityKey)> {
    // a singleton's key is empty, so it needs no `--key`
    let bytes = hex::decode(hex.unwrap_or_default())?;

    let key = E::Key::decode_key(&bytes).map_err(|err| match hex {
        Some(_) => anyhow!("not a {} key: {err}", E::NS),
        None => anyhow!("--key is required for this namespace"),
    })?;

    Ok((E::NS, EntityKey::of(&key)))
}
