use dolos::prelude::*;
use pallas::storage::rolldb::{chain, wal};
use std::path::Path;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: super::Config, _args: &Args) -> Result<(), Error> {
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
    .map_err(Error::config)?;

    let chain = chain::Store::open(rolldb_path.join("chain")).map_err(Error::config)?;

    dolos::serve::serve(config.serve, wal, chain).await?;

    Ok(())
}
