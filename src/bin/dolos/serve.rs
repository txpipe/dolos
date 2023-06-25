use dolos::prelude::*;
use dolos::storage::rolldb::RollDB;
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

    let db =
        RollDB::open(rolldb_path, config.rolldb.k_param.unwrap_or(1000)).map_err(Error::config)?;

    // placeholder while we make follow-tip optional
    let (_, from_sync) = gasket::messaging::tokio::broadcast_channel(100);

    dolos::serve::grpc::serve(config.serve.grpc, db, from_sync.try_into().unwrap()).await?;

    Ok(())
}
