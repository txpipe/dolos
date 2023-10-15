use std::path::Path;

use dolos::{prelude::*, storage::applydb::ApplyDB};
use pallas::storage::rolldb::{chain, wal};

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
    )
    .map_err(Error::storage)?;

    let chain = chain::Store::open(rolldb_path.join("chain")).map_err(Error::storage)?;

    let ledger = ApplyDB::open(rolldb_path.join("ledger")).map_err(Error::storage)?;

    Ok((wal, chain, ledger))
}
