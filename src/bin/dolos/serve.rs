use dolos::prelude::*;
use dolos::storage::rolldb::RollDB;
use std::path::Path;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: super::Config, _args: &Args) -> Result<(), Error> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish(),
    )
    .unwrap();

    let rolldb_path = config
        .rolldb
        .path
        .as_deref()
        .unwrap_or_else(|| Path::new("/rolldb"));

    let db = RollDB::open(
        rolldb_path,
        config.rolldb.k_param.unwrap_or(1000),
        config.rolldb.k_param_buffer.unwrap_or_default(),
    )
    .map_err(Error::config)?;

    dolos::serve::serve(config.serve, db).await?;

    Ok(())
}
