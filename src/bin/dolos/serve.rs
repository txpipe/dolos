use dolos::prelude::*;
use dolos::storage::rolldb::RollDB;
use futures_util::future::join_all;
use std::path::Path;
use tracing::info;

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

    let mut tasks = vec![];

    if let Some(grpc_config) = config.serve.grpc {
        tasks.push(tokio::spawn(dolos::serve::grpc::serve(
            grpc_config,
            db.clone(),
        )));
    } else {
        info!("no gRPC config found, not serving over gRPC")
    }

    if let Some(ouroboros_config) = config.serve.ouroboros {
        tasks.push(tokio::spawn(dolos::serve::ouroboros::serve(
            ouroboros_config,
            db,
            from_sync.clone().try_into().unwrap(),
        )));
    } else {
        info!("no ouroboros config found, not serving over ouroboros")
    }

    join_all(tasks).await;

    Ok(())
}
