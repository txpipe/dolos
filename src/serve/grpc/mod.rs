use dolos_core::config::GrpcConfig;
use pallas::interop::utxorpc::LedgerContext;
use tonic::transport::{Certificate, Server, ServerTlsConfig};
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

use crate::prelude::*;

pub(crate) mod block_refs;
mod convert;
mod masking;
mod stream;
mod v1alpha;
mod v1beta;

/// Applies the HTTP/2 transport tuning from [`GrpcConfig`] to the tonic server
/// builder.
///
/// Adaptive windowing (BDP-based) is on by default and, when enabled, makes the
/// explicit window sizes moot — hyper auto-sizes the stream/connection windows
/// to the link. The two modes are mutually exclusive in hyper, so we warn if
/// both are set rather than silently dropping the fixed sizes.
fn apply_http2_tuning(mut server: Server, cfg: &GrpcConfig) -> Server {
    if let Some(f) = cfg.http2_max_frame_size {
        server = server.max_frame_size(Some(f));
    }
    if let Some(s) = cfg.http2_max_concurrent_streams {
        server = server.max_concurrent_streams(Some(s));
    }

    if cfg.http2_adaptive_window() {
        if cfg.http2_initial_stream_window_size.is_some()
            || cfg.http2_initial_connection_window_size.is_some()
        {
            warn!(
                "grpc: http2_adaptive_window is enabled, so http2_initial_stream_window_size \
                 and http2_initial_connection_window_size are ignored"
            );
        }
        return server.http2_adaptive_window(Some(true));
    }

    if let Some(w) = cfg.http2_initial_stream_window_size {
        server = server.initial_stream_window_size(Some(w));
    }
    if let Some(w) = cfg.http2_initial_connection_window_size {
        server = server.initial_connection_window_size(Some(w));
    }

    server
}

#[derive(Clone)]
pub struct ContextAdapter<T: dolos_core::StateStore>(T);

pub struct Driver;

impl<D, C> dolos_core::Driver<D, C> for Driver
where
    D: Domain + LedgerContext,
    C: CancelToken,
{
    type Config = GrpcConfig;

    async fn run(cfg: Self::Config, domain: D, cancel: C) -> Result<(), ServeError> {
        let addr = cfg.listen_address.parse().unwrap();

        let sync_v1alpha = v1alpha::spec::sync::sync_service_server::SyncServiceServer::new(
            v1alpha::sync::SyncServiceImpl::new(domain.clone(), cancel.clone()),
        );
        let sync_v1beta = v1beta::spec::sync::sync_service_server::SyncServiceServer::new(
            v1beta::sync::SyncServiceImpl::new(domain.clone(), cancel.clone()),
        );

        let query_v1alpha = v1alpha::spec::query::query_service_server::QueryServiceServer::new(
            v1alpha::query::QueryServiceImpl::new(domain.clone()),
        );
        let query_v1beta = v1beta::spec::query::query_service_server::QueryServiceServer::new(
            v1beta::query::QueryServiceImpl::new(domain.clone()),
        );

        let watch_v1alpha = v1alpha::spec::watch::watch_service_server::WatchServiceServer::new(
            v1alpha::watch::WatchServiceImpl::new(domain.clone(), cancel.clone()),
        );
        let watch_v1beta = v1beta::spec::watch::watch_service_server::WatchServiceServer::new(
            v1beta::watch::WatchServiceImpl::new(domain.clone(), cancel.clone()),
        );

        let submit_v1alpha = v1alpha::spec::submit::submit_service_server::SubmitServiceServer::new(
            v1alpha::submit::SubmitServiceImpl::new(domain.clone()),
        );
        let submit_v1beta = v1beta::spec::submit::submit_service_server::SubmitServiceServer::new(
            v1beta::submit::SubmitServiceImpl::new(domain.clone()),
        );

        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(v1alpha::spec::cardano::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(v1alpha::spec::sync::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(v1alpha::spec::query::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(v1alpha::spec::submit::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(v1alpha::spec::watch::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(v1beta::spec::cardano::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(v1beta::spec::sync::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(v1beta::spec::query::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(v1beta::spec::submit::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(v1beta::spec::watch::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(protoc_wkt::google::protobuf::FILE_DESCRIPTOR_SET)
            .build_v1()
            .unwrap();

        let cors_layer = if cfg.permissive_cors() {
            CorsLayer::permissive()
        } else {
            CorsLayer::new()
        };

        let builder = apply_http2_tuning(Server::builder().accept_http1(true), &cfg);

        let mut server = builder.layer(cors_layer);

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
            .add_service(tonic_web::enable(sync_v1alpha))
            .add_service(tonic_web::enable(sync_v1beta))
            .add_service(tonic_web::enable(query_v1alpha))
            .add_service(tonic_web::enable(query_v1beta))
            .add_service(tonic_web::enable(submit_v1alpha))
            .add_service(tonic_web::enable(submit_v1beta))
            .add_service(tonic_web::enable(watch_v1alpha))
            .add_service(tonic_web::enable(watch_v1beta))
            .add_service(reflection)
            .serve_with_shutdown(addr, cancel.cancelled())
            .await
            .map_err(|e| ServeError::Internal(e.into()))?;

        Ok(())
    }
}
