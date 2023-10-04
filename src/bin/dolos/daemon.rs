use std::path::Path;

use dolos::{
    prelude::*,
    storage::{applydb::ApplyDB, rolldb::RollDB},
};

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(
    config: super::Config,
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

    let rolldb =
        RollDB::open(rolldb_path, config.rolldb.k_param.unwrap_or(1000)).map_err(Error::storage)?;

    let applydb_path = config
        .applydb
        .path
        .as_deref()
        .unwrap_or_else(|| Path::new("/applydb"));

    let byron_genesis =
        pallas::ledger::configs::byron::from_file(&config.byron.path).map_err(Error::config)?;

    let applydb = ApplyDB::open(applydb_path).map_err(Error::storage)?;

    if applydb.is_empty() {
        applydb
            .insert_genesis_utxos(&byron_genesis)
            .map_err(Error::storage)?;
    }

    let server = tokio::spawn(dolos::serve::serve(config.serve, rolldb.clone()));

    dolos::sync::pipeline(&config.upstream, rolldb, applydb, policy)
        .unwrap()
        .block();

    server.abort();

    Ok(())
}
