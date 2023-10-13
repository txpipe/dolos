use std::path::Path;

use dolos::{prelude::*, storage::applydb::ApplyDB};
use pallas::storage::rolldb::{chain, wal};

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(
    config: &super::Config,
    policy: &gasket::runtime::Policy,
    _args: &Args,
) -> Result<(), Error> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish(),
    )
    .unwrap();

    let rolldb_path = config
        .rolldb
        .path
        .as_deref()
        .unwrap_or_else(|| Path::new("/rolldb"));

    let wal = wal::Store::open(
        rolldb_path.join("wal"),
        config.rolldb.k_param.unwrap_or(1000),
    )
    .map_err(Error::storage)?;

    let chain = chain::Store::open(rolldb_path.join("chain")).map_err(Error::storage)?;

    let ledger = ApplyDB::open(rolldb_path.join("ledger")).map_err(Error::storage)?;

    let byron_genesis =
        pallas::ledger::configs::byron::from_file(&config.byron.path).map_err(Error::config)?;

    dolos::sync::pipeline(&config.upstream, wal, chain, ledger, byron_genesis, policy)
        .unwrap()
        .block();

    Ok(())
}
