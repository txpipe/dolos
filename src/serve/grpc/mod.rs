use std::{path::PathBuf, sync::Arc};

use pallas::storage::rolldb::{chain, wal};
use serde::{Deserialize, Serialize};
use tonic::transport::{Certificate, Server, ServerTlsConfig};

use tracing::info;
use utxorpc_spec::utxorpc::v1alpha as u5c;

use crate::{prelude::*, submit::Transaction};

mod submit;
mod sync;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    listen_address: String,
    tls_client_ca_root: Option<PathBuf>,
}

pub async fn serve(
    config: Config,
    wal: wal::Store,
    chain: chain::Store,
    mempool: Arc<crate::submit::MempoolState>,
    txs_out: gasket::messaging::tokio::ChannelSendAdapter<Vec<Transaction>>,
) -> Result<(), Error> {
    let addr = config.listen_address.parse().unwrap();

    let sync_service = sync::ChainSyncServiceImpl::new(wal, chain);
    let sync_service =
        u5c::sync::chain_sync_service_server::ChainSyncServiceServer::new(sync_service);

    let submit_service = submit::SubmitServiceImpl::new(txs_out, mempool);
    let submit_service =
        u5c::submit::submit_service_server::SubmitServiceServer::new(submit_service);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(u5c::cardano::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(u5c::sync::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(u5c::submit::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(protoc_wkt::google::protobuf::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    let mut server = Server::builder().accept_http1(true);

    if let Some(pem) = config.tls_client_ca_root {
        let pem = std::env::current_dir().unwrap().join(pem);
        let pem = std::fs::read_to_string(pem).map_err(Error::config)?;
        let pem = Certificate::from_pem(pem);

        let tls = ServerTlsConfig::new().client_ca_root(pem);

        server = server.tls_config(tls).map_err(Error::config)?;
    }

    info!("serving via gRPC on address: {}", config.listen_address);

    // to allow GrpcWeb we must enable http1
    server
        .add_service(tonic_web::enable(sync_service))
        .add_service(tonic_web::enable(submit_service))
        .add_service(reflection)
        .serve(addr)
        .await
        .map_err(Error::server)?;

    Ok(())
}
