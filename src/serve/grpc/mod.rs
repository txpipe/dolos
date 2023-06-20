use serde::{Deserialize, Serialize};
use tonic::transport::Server;

use utxorpc::proto::sync::v1::chain_sync_service_server::ChainSyncServiceServer;

use crate::prelude::*;
use crate::storage::rolldb::RollDB;

mod sync;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    listen_address: String,
}

pub async fn serve(config: Config, db: RollDB) -> Result<(), Error> {
    let addr = config.listen_address.parse().unwrap();
    let service = sync::ChainSyncServiceImpl::new(db);
    let server = ChainSyncServiceServer::new(service);

    Server::builder()
        .accept_http1(true)
        // GrpcWeb is over http1 so we must enable it.
        .add_service(tonic_web::enable(server))
        .serve(addr)
        .await
        .map_err(Error::server)?;

    Ok(())
}
