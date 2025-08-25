use pallas::interop::utxorpc::{spec as u5c, LedgerContext};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tonic::transport::{Certificate, Server, ServerTlsConfig};
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::prelude::*;

mod convert;
mod iterator;
mod masking;
mod query;
mod stream;
mod submit;
mod sync;
mod watch;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub listen_address: String,
    pub tls_client_ca_root: Option<PathBuf>,
    pub permissive_cors: Option<bool>,
}

#[derive(Clone)]
pub struct ContextAdapter<T: dolos_core::StateStore>(T);

impl<T: dolos_core::StateStore> pallas::interop::utxorpc::LedgerContext for ContextAdapter<T> {
    fn get_utxos<'a>(
        &self,
        refs: &[pallas::interop::utxorpc::TxoRef],
    ) -> Option<pallas::interop::utxorpc::UtxoMap> {
        let refs: Vec<_> = refs.iter().map(|x| TxoRef::from(*x)).collect();

        let some = self
            .0
            .get_utxos(refs)
            .ok()?
            .into_iter()
            .map(|(k, v)| {
                let era = v.0.try_into().expect("era out of range");
                (k.into(), (era, v.1))
            })
            .collect();

        Some(some)
    }
}

pub struct Driver;

impl<D: Domain, C: CancelToken> dolos_core::Driver<D, C> for Driver
where
    D::State: LedgerContext,
{
    type Config = Config;

    async fn run(cfg: Self::Config, domain: D, cancel: C) -> Result<(), ServeError> {
        let addr = cfg.listen_address.parse().unwrap();

        let sync_service = sync::SyncServiceImpl::new(domain.clone(), cancel.clone());
        let sync_service = u5c::sync::sync_service_server::SyncServiceServer::new(sync_service);

        let query_service = query::QueryServiceImpl::new(domain.clone());
        let query_service =
            u5c::query::query_service_server::QueryServiceServer::new(query_service);

        let watch_service = watch::WatchServiceImpl::new(domain.clone(), cancel.clone());
        let watch_service =
            u5c::watch::watch_service_server::WatchServiceServer::new(watch_service);

        let submit_service = submit::SubmitServiceImpl::new(domain.clone());
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

        let cors_layer = if cfg.permissive_cors.unwrap_or_default() {
            CorsLayer::permissive()
        } else {
            CorsLayer::new()
        };

        let mut server = Server::builder().accept_http1(true).layer(cors_layer);

        if let Some(pem) = &cfg.tls_client_ca_root {
            let pem = std::env::current_dir().unwrap().join(pem);
            let pem = std::fs::read_to_string(pem).map_err(|e| ServeError::Internal(e.into()))?;
            let pem = Certificate::from_pem(pem);

            let tls = ServerTlsConfig::new().client_ca_root(pem);

            server = server
                .tls_config(tls)
                .map_err(|e| ServeError::Internal(e.into()))?;
        }

        info!("serving via gRPC on address: {}", cfg.listen_address);

        // to allow GrpcWeb we must enable http1
        server
            .add_service(tonic_web::enable(sync_service))
            .add_service(tonic_web::enable(query_service))
            .add_service(tonic_web::enable(submit_service))
            .add_service(tonic_web::enable(watch_service))
            .add_service(reflection)
            .serve_with_shutdown(addr, cancel.cancelled())
            .await
            .map_err(|e| ServeError::Internal(e.into()))?;

        Ok(())
    }
}
