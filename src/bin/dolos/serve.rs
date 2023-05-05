use std::path::Path;

use dolos::downstream::ChainSyncServiceImpl;
use dolos::prelude::*;
use dolos::rolldb::RollDB;
use tonic::transport::Server;
use utxorpc::proto::sync::v1::*;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: &super::Config, _args: &Args) -> Result<(), Error> {
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
        .unwrap_or_else(|| Path::new("/db"));

    let db = RollDB::open(rolldb_path).map_err(Error::config)?;

    let addr = "127.0.0.1:50051".parse().unwrap();
    let service = ChainSyncServiceImpl::new(db);

    Server::builder()
        .add_service(chain_sync_service_server::ChainSyncServiceServer::new(
            service,
        ))
        .serve(addr)
        .await
        .map_err(Error::server)?;

    Ok(())
}
