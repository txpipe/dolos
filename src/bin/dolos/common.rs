use pallas::storage::rolldb::{chain, wal};
use std::path::Path;
use tracing::Level;
use tracing_subscriber::{filter::Targets, prelude::*};

use dolos::{prelude::*, storage::applydb::ApplyDB};

use crate::LoggingConfig;

fn define_rolldb_path(config: &crate::Config) -> &Path {
    config
        .rolldb
        .path
        .as_deref()
        .unwrap_or_else(|| Path::new("/rolldb"))
}

pub type Stores = (wal::Store, chain::Store, ApplyDB);

pub fn open_data_stores(config: &crate::Config) -> Result<Stores, Error> {
    let rolldb_path = define_rolldb_path(config);

    let wal = wal::Store::open(
        rolldb_path.join("wal"),
        config.rolldb.k_param.unwrap_or(1000),
        config.rolldb.immutable_overlap,
    )
    .map_err(Error::storage)?;

    let chain = chain::Store::open(rolldb_path.join("chain")).map_err(Error::storage)?;

    let ledger = ApplyDB::open(rolldb_path.join("ledger")).map_err(Error::storage)?;

    Ok((wal, chain, ledger))
}

#[allow(dead_code)]
pub fn destroy_data_stores(config: &crate::Config) -> Result<(), Error> {
    let rolldb_path = define_rolldb_path(config);

    wal::Store::destroy(rolldb_path.join("wal")).map_err(Error::storage)?;
    chain::Store::destroy(rolldb_path.join("chain")).map_err(Error::storage)?;
    ApplyDB::destroy(rolldb_path.join("ledger")).map_err(Error::storage)?;

    Ok(())
}

pub fn setup_tracing(config: &LoggingConfig) -> miette::Result<()> {
    let level = config.max_level.unwrap_or(Level::INFO);

    let mut filter = Targets::new()
        .with_target("dolos", level)
        .with_target("gasket", level);

    if config.include_pallas {
        filter = filter.with_target("pallas", level);
    }

    if config.include_grpc {
        filter = filter.with_target("tonic", level);
    }

    tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(level)
        .finish()
        .with(filter)
        .init();

    Ok(())
}
