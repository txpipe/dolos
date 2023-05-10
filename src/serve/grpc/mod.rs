use tonic::transport::Server;

use crate::prelude::*;
use crate::rolldb::RollDB;

mod sync;

pub async fn serve(db: RollDB) -> Result<(), Error> {
    let addr = "127.0.0.1:50051".parse().unwrap();
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
