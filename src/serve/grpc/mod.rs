use serde::{Deserialize, Serialize};
use tonic::transport::Server;

use crate::prelude::*;
use crate::rolldb::RollDB;

mod sync;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    listen_address: String,
}

pub async fn serve(config: Config, db: RollDB) -> Result<(), Error> {
    let addr = config.listen_address.parse().unwrap();
    let service = sync::ChainSyncServiceImpl::new(db);

    Server::builder()
        .add_service(
            utxorpc::proto::sync::v1::chain_sync_service_server::ChainSyncServiceServer::new(
                service,
            ),
        )
        .serve(addr)
        .await
        .map_err(Error::server)?;

    Ok(())
}
