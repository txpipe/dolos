use std::path::PathBuf;

use pallas::storage::rolldb::{chain, wal};
use serde::{Deserialize, Serialize};
use tonic::transport::{Certificate, Server, ServerTlsConfig};

use tracing::info;
use utxorpc::proto::sync::v1::chain_sync_service_server::ChainSyncServiceServer;

use crate::prelude::*;

mod sync;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    listen_address: String,
    tls_client_ca_root: Option<PathBuf>,
}

pub async fn serve(config: Config, wal: wal::Store, chain: chain::Store) -> Result<(), Error> {
    let addr = config.listen_address.parse().unwrap();
    let service = sync::ChainSyncServiceImpl::new(wal, chain);
    let service = ChainSyncServiceServer::new(service);

    let mut server = Server::builder().accept_http1(true);

    if let Some(pem) = config.tls_client_ca_root {
        let pem = std::env::current_dir().unwrap().join(pem);
        let pem = std::fs::read_to_string(pem).map_err(Error::config)?;
        let pem = Certificate::from_pem(pem);

        let tls = ServerTlsConfig::new().client_ca_root(pem);

        server = server.tls_config(tls).map_err(Error::config)?;
    }

    info!("serving via gRPC on address: {}", config.listen_address);

    server
        // GrpcWeb is over http1 so we must enable it.
        .add_service(tonic_web::enable(service))
        .serve(addr)
        .await
        .map_err(Error::server)?;

    Ok(())
}
