use pallas::interop::utxorpc::spec as u5c;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Server, ServerTlsConfig};
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::ledger::pparams::Genesis;
use crate::mempool::Mempool;
use crate::prelude::*;
use crate::state::LedgerStore;
use crate::wal::redb::WalStore;

mod convert;
mod query;
mod submit;
mod sync;
mod watch;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub listen_address: String,
    pub tls_client_ca_root: Option<PathBuf>,
    pub permissive_cors: Option<bool>,
}

pub async fn serve(
    config: Config,
    genesis: Arc<Genesis>,
    wal: WalStore,
    ledger: LedgerStore,
    mempool: Mempool,
    exit: CancellationToken,
) -> Result<(), Error> {
    let addr = config.listen_address.parse().unwrap();

    let sync_service = sync::SyncServiceImpl::new(wal.clone(), ledger.clone());
    let sync_service = u5c::sync::sync_service_server::SyncServiceServer::new(sync_service);

    let query_service = query::QueryServiceImpl::new(ledger.clone(), genesis.clone());
    let query_service = u5c::query::query_service_server::QueryServiceServer::new(query_service);

    let watch_service = watch::WatchServiceImpl::new(wal.clone(), ledger.clone());
    let watch_service = u5c::watch::watch_service_server::WatchServiceServer::new(watch_service);

    let submit_service = submit::SubmitServiceImpl::new(mempool, ledger.clone());
    let submit_service =
        u5c::submit::submit_service_server::SubmitServiceServer::new(submit_service);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(u5c::cardano::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(u5c::sync::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(u5c::query::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(u5c::submit::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(u5c::watch::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(protoc_wkt::google::protobuf::FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    let cors_layer = if config.permissive_cors.unwrap_or_default() {
        CorsLayer::permissive()
    } else {
        CorsLayer::new()
    };

    let mut server = Server::builder().accept_http1(true).layer(cors_layer);

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
        .add_service(tonic_web::enable(query_service))
        .add_service(tonic_web::enable(submit_service))
        .add_service(tonic_web::enable(watch_service))
        .add_service(reflection)
        .serve_with_shutdown(addr, exit.cancelled())
        .await
        .map_err(Error::server)?;

    Ok(())
}
